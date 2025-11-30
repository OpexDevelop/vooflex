import express from 'express';
import cors from 'cors';
import swaggerUi from 'swagger-ui-express';
import YAML from 'yamljs';
import path from 'path';
import { fileURLToPath } from 'url';
import pg from 'pg';
import { getAuthorData, getAuthorFilters } from 'stihirus-reader';

const { Pool } = pg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = process.env.PORT || 30010;

app.use(cors());

// --- DATABASE CONFIGURATION ---
const pool = new Pool({
    connectionString: process.env.POSTGRES_URL, 
    ssl: { rejectUnauthorized: false }, 
    max: 20, 
    idleTimeoutMillis: 30000
});

// --- INIT DB ---
async function initDB() {
    const client = await pool.connect();
    try {
        await client.query(`
            CREATE TABLE IF NOT EXISTS request_logs (
                id SERIAL PRIMARY KEY,
                method VARCHAR(10),
                path TEXT,
                status INTEGER,
                duration_ms INTEGER,
                ip VARCHAR(50),
                user_agent TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_logs_created ON request_logs(created_at);`);

        await client.query(`
            CREATE TABLE IF NOT EXISTS authors (
                id SERIAL PRIMARY KEY,
                identifier VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(255),
                real_name VARCHAR(255),
                profile_url TEXT,
                avatar_url TEXT,
                description TEXT,
                stats JSONB, 
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        `);

        await client.query(`
            CREATE TABLE IF NOT EXISTS poems (
                id BIGINT PRIMARY KEY,
                author_identifier VARCHAR(255) REFERENCES authors(identifier) ON DELETE CASCADE,
                title TEXT,
                text TEXT,
                created_str VARCHAR(50), 
                rubric_name TEXT,
                metadata JSONB, 
                fetched_at TIMESTAMPTZ DEFAULT NOW()
            );
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_poems_author ON poems(author_identifier);`);
        
        console.log("DB Schema & Logs initialized.");
    } catch (err) {
        console.error("DB Init Error:", err);
    } finally {
        client.release();
    }
}
initDB();

// --- LOGGER ---
app.use(async (req, res, next) => {
    const start = Date.now();
    const originalEnd = res.end;
    res.end = function (...args) {
        const duration = Date.now() - start;
        const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
        pool.query(`
            INSERT INTO request_logs (method, path, status, duration_ms, ip, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6)
        `, [req.method, req.originalUrl, res.statusCode, duration, ip, req.headers['user-agent']])
        .catch(err => console.error("Log fail:", err));
        originalEnd.apply(res, args);
    };
    next();
});

// --- SWAGGER SETUP ---
let swaggerDocument;
try {
    swaggerDocument = YAML.load(path.join(process.cwd(), 'openapi.yaml'));
} catch (e) {
    try {
        swaggerDocument = YAML.load(path.join(__dirname, 'openapi.yaml'));
    } catch (e2) {
        console.error("Swagger YAML not found:", e2);
    }
}

if (swaggerDocument) {
    const swaggerOptions = {
        customCssUrl: 'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.11.0/swagger-ui.min.css',
        customJs: [
            'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.11.0/swagger-ui-bundle.min.js',
            'https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/5.11.0/swagger-ui-standalone-preset.min.js'
        ],
        customSiteTitle: "StihiRus API Docs"
    };
    app.use('/docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument, swaggerOptions));
}

// --- HELPERS ---

// Трекер активных синхронизаций, чтобы не запускать дубликаты
const activeSyncs = new Set();

async function savePageToDB(identifier, data) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        // Save Author Info
        await client.query(`
            INSERT INTO authors (identifier, username, real_name, profile_url, avatar_url, description, stats, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (identifier) DO UPDATE 
            SET username=EXCLUDED.username, stats=EXCLUDED.stats, updated_at=NOW();
        `, [identifier, data.username, data.canonicalUsername, data.profileUrl, data.avatarUrl, data.description, JSON.stringify(data.stats)]);

        // Save Poems
        if (data.poems && data.poems.length > 0) {
            for (const poem of data.poems) {
                const meta = {
                    collection: poem.collection,
                    rating: poem.rating,
                    commentsCount: poem.commentsCount,
                    imageUrl: poem.imageUrl,
                    hasCertificate: poem.hasCertificate,
                    rubricUrl: poem.rubric?.url
                };
                await client.query(`
                    INSERT INTO poems (id, author_identifier, title, text, created_str, rubric_name, metadata, fetched_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                    ON CONFLICT (id) DO UPDATE
                    SET title=EXCLUDED.title, text=EXCLUDED.text, metadata=EXCLUDED.metadata, fetched_at=NOW();
                `, [poem.id, identifier, poem.title, poem.text, poem.created, poem.rubric?.name, JSON.stringify(meta)]);
            }
        }
        await client.query('COMMIT');
    } catch (e) {
        await client.query('ROLLBACK');
        console.error(`[${identifier}] Save Error:`, e.message);
    } finally {
        client.release();
    }
}

async function checkExistingIds(ids) {
    if (ids.length === 0) return [];
    const query = `SELECT id FROM poems WHERE id = ANY($1::bigint[])`;
    const res = await pool.query(query, [ids]);
    return res.rows.map(r => parseInt(r.id));
}

/**
 * ФОНОВАЯ ЗАДАЧА СИНХРОНИЗАЦИИ
 * Работает отдельно от ответа пользователю.
 */
async function runBackgroundSync(identifier, delayMs) {
    if (activeSyncs.has(identifier)) {
        console.log(`[${identifier}] Sync already running in background.`);
        return;
    }

    activeSyncs.add(identifier);
    console.log(`[${identifier}] Starting Background Smart Sync...`);

    try {
        let currentPage = 1;
        let keepFetching = true;

        while (keepFetching) {
            // Качаем страницу
            const res = await getAuthorData(identifier, currentPage, delayMs);
            
            if (res.status !== 'success' || !res.data.poems || res.data.poems.length === 0) {
                console.log(`[${identifier}] Sync reached end or error at page ${currentPage}.`);
                break;
            }

            // Сохраняем
            await savePageToDB(identifier, res.data);

            // Проверяем, есть ли эти стихи уже в базе (Smart Sync logic)
            const fetchedIds = res.data.poems.map(p => p.id);
            const existingIds = await checkExistingIds(fetchedIds);

            // Если ВСЕ стихи на этой странице уже были в базе, значит дальше старое
            if (existingIds.length === fetchedIds.length && currentPage > 1) {
                console.log(`[${identifier}] All poems on page ${currentPage} exist. Sync complete.`);
                keepFetching = false;
            } else {
                // Иначе продолжаем качать следующую страницу
                console.log(`[${identifier}] Page ${currentPage} synced. Moving to next...`);
                currentPage++;
                
                // Пауза перед следующим запросом, чтобы не забанили
                await new Promise(resolve => setTimeout(resolve, delayMs));
            }

            // SAFETY BREAK (чтобы не уйти в бесконечный цикл при ошибках)
            if (currentPage > 100) keepFetching = false; 
        }

    } catch (err) {
        console.error(`[${identifier}] Background Sync Failed:`, err.message);
    } finally {
        activeSyncs.delete(identifier);
    }
}

// --- DB QUERY HELPERS ---
async function getFilteredPoemsFromDB(identifier, queryParams) {
    const client = await pool.connect();
    try {
        let sql = `SELECT * FROM poems WHERE author_identifier = $1`;
        let values = [identifier];
        let paramIndex = 2;

        const { q, searchFields, sort, year, rubric, collection } = queryParams;

        // Filters
        if (rubric && rubric !== 'all') {
            sql += ` AND rubric_name = $${paramIndex}`;
            values.push(rubric);
            paramIndex++;
        }
        if (collection && collection !== 'all') {
            if (collection === 'Без сборника') {
                sql += ` AND (metadata->>'collection' IS NULL OR metadata->>'collection' = 'null')`;
            } else {
                sql += ` AND metadata->>'collection' = $${paramIndex}`;
                values.push(collection);
                paramIndex++;
            }
        }
        if (year && year !== 'all') {
            sql += ` AND created_str LIKE $${paramIndex}`;
            values.push(`%${year}%`);
            paramIndex++;
        }

        // Search
        if (q) {
            const fieldsToCheck = searchFields ? searchFields.split(',') : ['title', 'text'];
            const searchConditions = [];
            fieldsToCheck.forEach(field => {
                const f = field.trim();
                if (f === 'title') searchConditions.push(`title ILIKE $${paramIndex}`);
                if (f === 'text') searchConditions.push(`text ILIKE $${paramIndex}`);
                if (f === 'rubric') searchConditions.push(`rubric_name ILIKE $${paramIndex}`);
            });
            if (searchConditions.length > 0) {
                sql += ` AND (${searchConditions.join(' OR ')})`;
                values.push(`%${q}%`);
                paramIndex++;
            }
        }

        // Sorting
        let orderBy = `ORDER BY id DESC`; 
        if (sort) {
            switch (sort) {
                case 'date_asc': orderBy = `ORDER BY id ASC`; break;
                case 'date_desc': orderBy = `ORDER BY id DESC`; break;
                case 'popular':
                case 'rating_desc': orderBy = `ORDER BY COALESCE((metadata->>'rating')::int, 0) DESC, id DESC`; break;
                case 'rating_asc': orderBy = `ORDER BY COALESCE((metadata->>'rating')::int, 0) ASC, id DESC`; break;
                case 'title_asc': orderBy = `ORDER BY title ASC`; break;
                case 'title_desc': orderBy = `ORDER BY title DESC`; break;
                default: orderBy = `ORDER BY id DESC`;
            }
        }

        sql += ` ${orderBy}`;

        // Pagination for DB query (simulated)
        // Note: For large DBs, use OFFSET/LIMIT in SQL. 
        // Here we fetch filtered list and let frontend handle basic pagination or return full list if needed
        // but to prevent memory boom on big authors, let's limit simply:
        sql += ` LIMIT 1000`; 

        const res = await client.query(sql, values);
        
        const authorRes = await client.query(`SELECT * FROM authors WHERE identifier = $1`, [identifier]);
        if (authorRes.rows.length === 0) return null;
        const author = authorRes.rows[0];

        const poems = res.rows.map(row => ({
            id: parseInt(row.id),
            title: row.title,
            text: row.text,
            created: row.created_str,
            rubric: { name: row.rubric_name, url: row.metadata.rubricUrl },
            collection: row.metadata.collection,
            rating: row.metadata.rating,
            commentsCount: row.metadata.commentsCount,
            imageUrl: row.metadata.imageUrl,
            hasCertificate: row.metadata.hasCertificate
        }));

        return {
            status: 'success',
            data: {
                authorId: 0,
                username: author.username,
                canonicalUsername: author.username,
                profileUrl: author.profile_url,
                avatarUrl: author.avatar_url,
                description: author.description,
                stats: author.stats,
                collections: [], 
                poems: poems
            }
        };

    } finally {
        client.release();
    }
}


// --- ROUTES ---

// Static
app.get('/', (req, res) => res.sendFile(path.join(process.cwd(), 'public', 'index.html')));
app.get('/stihi', (req, res) => res.sendFile(path.join(process.cwd(), 'public', 'stihi.html')));
app.get('/robots.txt', (req, res) => {
    res.type('text/plain').send(`User-agent: *\nAllow: /$\nAllow: /stihi\nAllow: /docs\nDisallow: /`);
});

// Logs
app.get('/logs', async (req, res) => {
    try {
        const result = await pool.query(`SELECT * FROM request_logs ORDER BY id DESC LIMIT 500`);
        res.type('text/plain');
        let output = "--- SERVER LOGS ---\n";
        result.rows.forEach(row => {
            const date = new Date(row.created_at).toISOString();
            output += `[${date}] [${row.method}] [${row.status}] [${row.duration_ms}ms] ${row.path}\n`;
        });
        res.send(output);
    } catch (e) { res.status(500).send("Log Error"); }
});

// Stats
app.get('/stats', async (req, res) => {
    try {
        const client = await pool.connect();
        const count = async (t) => (await client.query(`SELECT COUNT(*) FROM ${t}`)).rows[0].count;
        const [totalReq, authors, poems] = await Promise.all([
            count('request_logs'), count('authors'), count('poems')
        ]);
        client.release();
        res.type('text/plain').send(`Requests: ${totalReq}\nAuthors: ${authors}\nPoems: ${poems}`);
    } catch (e) { res.status(500).send("Stats Error"); }
});


// === MAIN API ENDPOINT (REWRITTEN) ===
app.get('/author/:identifier', async (req, res) => {
    const identifier = req.params.identifier;
    let page = parseInt(req.query.page) || 1;
    let delayMs = req.query.delay ? parseInt(req.query.delay) : 500;
    
    // Флаг: есть ли фильтры? (Если да - нам нужна БД, а не просто страница с источника)
    const hasFilters = req.query.q || (req.query.rubric && req.query.rubric !== 'all') || 
                       (req.query.year && req.query.year !== 'all') || 
                       (req.query.collection && req.query.collection !== 'all') ||
                       (req.query.sort && req.query.sort !== 'date_desc');

    try {
        // 1. Сначала скачиваем запрошенную страницу с источника (для свежести)
        //    Если запрошены фильтры, мы все равно качаем page=1 для обновления "головы" списка
        const fetchPage = hasFilters ? 1 : page;
        const freshData = await getAuthorData(identifier, fetchPage, delayMs);

        if (freshData.status === 'success') {
            // 2. Сохраняем свежие данные в БД
            await savePageToDB(identifier, freshData.data);
            
            // 3. ЗАПУСКАЕМ ФОНОВУЮ СИНХРОНИЗАЦИЮ (Background Task)
            //    Не ждем (await), просто запускаем.
            runBackgroundSync(identifier, delayMs);
        } else {
            console.error(`[${identifier}] Failed to fetch page ${fetchPage}`);
        }

        // 4. Формируем ответ пользователю
        
        if (hasFilters) {
            // ВАРИАНТ А: Пользователь что-то ищет или фильтрует.
            // Источник (Stihi.ru) не умеет фильтровать по JSON API, поэтому берем из нашей БД.
            const dbResponse = await getFilteredPoemsFromDB(identifier, req.query);
            if (dbResponse) {
                return res.json(dbResponse);
            } else {
                // Если в БД еще пусто, но данные скачались - отдаем свежие
                return res.json(freshData);
            }
        } else {
            // ВАРИАНТ Б: Обычный просмотр страниц.
            // Отдаем то, что только что скачали. Это максимально быстро и свежо.
            // Если была ошибка скачивания (freshData.status !== success), пробуем отдать из БД.
            
            if (freshData.status === 'success') {
                return res.json(freshData);
            } else {
                // Если источник лежит, пробуем отдать из кэша БД
                const dbBackup = await getFilteredPoemsFromDB(identifier, req.query);
                if (dbBackup) return res.json(dbBackup);
                
                return res.status(500).json(freshData);
            }
        }

    } catch (err) {
        console.error("Route Error:", err);
        return res.status(500).json({ status: 'error', error: { code: 500, message: err.message } });
    }
});

app.get('/author/:identifier/filters', async (req, res) => {
    try {
        const result = await getAuthorFilters(req.params.identifier);
        res.json(result);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

if (process.env.NODE_ENV !== 'production' && !process.env.VERCEL) {
    app.listen(port, () => console.log(`Server listening on port: ${port}`));
}

export default app;

