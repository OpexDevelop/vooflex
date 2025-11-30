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

// --- DB SETUP ---
const pool = new Pool({
    connectionString: process.env.POSTGRES_URL, 
    ssl: { rejectUnauthorized: false }, 
    max: 20, 
    idleTimeoutMillis: 30000
});

async function initDB() {
    const client = await pool.connect();
    try {
        await client.query(`
            CREATE TABLE IF NOT EXISTS request_logs (
                id SERIAL PRIMARY KEY,
                method VARCHAR(10), path TEXT, status INTEGER, duration_ms INTEGER,
                ip VARCHAR(50), user_agent TEXT, created_at TIMESTAMPTZ DEFAULT NOW()
            );
        `);
        await client.query(`
            CREATE TABLE IF NOT EXISTS authors (
                id SERIAL PRIMARY KEY,
                identifier VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(255), real_name VARCHAR(255), profile_url TEXT,
                avatar_url TEXT, description TEXT, stats JSONB, updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        `);
        await client.query(`
            CREATE TABLE IF NOT EXISTS poems (
                id BIGINT PRIMARY KEY,
                author_identifier VARCHAR(255) REFERENCES authors(identifier) ON DELETE CASCADE,
                title TEXT, text TEXT, created_str VARCHAR(50), rubric_name TEXT,
                metadata JSONB, fetched_at TIMESTAMPTZ DEFAULT NOW()
            );
        `);
    } catch (err) { console.error("DB Init Error:", err); } 
    finally { client.release(); }
}
initDB();

app.use(async (req, res, next) => {
    const start = Date.now();
    const originalEnd = res.end;
    res.end = function (...args) {
        const duration = Date.now() - start;
        pool.query(`INSERT INTO request_logs (method, path, status, duration_ms, ip, user_agent) VALUES ($1, $2, $3, $4, $5, $6)`, 
            [req.method, req.originalUrl, res.statusCode, duration, req.headers['x-forwarded-for'], req.headers['user-agent']]).catch(()=>{});
        originalEnd.apply(res, args);
    };
    next();
});

// --- HELPERS ---

async function savePageToDB(identifier, data) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        await client.query(`
            INSERT INTO authors (identifier, username, real_name, profile_url, avatar_url, description, stats, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (identifier) DO UPDATE SET username=EXCLUDED.username, stats=EXCLUDED.stats, updated_at=NOW();
        `, [identifier, data.username, data.canonicalUsername, data.profileUrl, data.avatarUrl, data.description, JSON.stringify(data.stats)]);

        if (data.poems?.length) {
            for (const poem of data.poems) {
                const meta = {
                    collection: poem.collection, rating: poem.rating, commentsCount: poem.commentsCount,
                    imageUrl: poem.imageUrl, hasCertificate: poem.hasCertificate, rubricUrl: poem.rubric?.url
                };
                await client.query(`
                    INSERT INTO poems (id, author_identifier, title, text, created_str, rubric_name, metadata, fetched_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                    ON CONFLICT (id) DO UPDATE SET title=EXCLUDED.title, text=EXCLUDED.text, metadata=EXCLUDED.metadata;
                `, [poem.id, identifier, poem.title, poem.text, poem.created, poem.rubric?.name, JSON.stringify(meta)]);
            }
        }
        await client.query('COMMIT');
    } catch (e) { await client.query('ROLLBACK'); console.error(e); } 
    finally { client.release(); }
}

async function checkExistingIds(ids) {
    if (!ids.length) return [];
    const res = await pool.query(`SELECT id FROM poems WHERE id = ANY($1::bigint[])`, [ids]);
    return res.rows.map(r => parseInt(r.id));
}

// --- CORE ROUTES ---

// 1. MAIN FETCH ROUTE (Мгновенный ответ)
app.get('/author/:identifier', async (req, res) => {
    const identifier = req.params.identifier;
    let page = parseInt(req.query.page) || 1;
    let delayMs = parseInt(req.query.delay) || 500;
    
    // Если есть фильтры - всегда берем из БД, но сначала обновляем "голову" (стр 1)
    const hasFilters = req.query.q || (req.query.rubric && req.query.rubric !== 'all') || 
                       (req.query.year && req.query.year !== 'all') || (req.query.collection && req.query.collection !== 'all');

    try {
        // Качаем запрошенную страницу (или первую, если фильтры) для свежести
        const fetchPage = hasFilters ? 1 : page;
        const freshData = await getAuthorData(identifier, fetchPage, delayMs);

        if (freshData.status === 'success') {
            await savePageToDB(identifier, freshData.data);
            // ПРИМЕЧАНИЕ: Мы НЕ запускаем тут runBackgroundSync, так как Vercel убьет его.
            // Синхронизация запускается отдельным запросом от клиента.
        }

        if (hasFilters) {
            const dbResponse = await getFilteredPoemsFromDB(identifier, req.query);
            return res.json(dbResponse || freshData);
        } else {
            if (freshData.status === 'success') return res.json(freshData);
            const dbBackup = await getFilteredPoemsFromDB(identifier, req.query);
            return dbBackup ? res.json(dbBackup) : res.status(500).json(freshData);
        }
    } catch (err) {
        return res.status(500).json({ status: 'error', error: { code: 500, message: err.message } });
    }
});

// 2. BACKGROUND SYNC ROUTE (Дергается клиентом)
// Этот маршрут "висит", пока идет синхронизация, чтобы Vercel не убил процесс.
const activeSyncs = new Set();

app.get('/author/:identifier/sync', async (req, res) => {
    const identifier = req.params.identifier;
    const delayMs = parseInt(req.query.delay) || 500;

    if (activeSyncs.has(identifier)) {
        return res.json({ status: 'skipped', message: 'Sync already running' });
    }
    activeSyncs.add(identifier);

    try {
        // Начинаем со 2-й страницы, т.к. 1-ю обычно качает основной запрос
        // Но для надежности проверяем с 1-й, если база пустая
        let currentPage = 1; 
        let syncedPages = 0;
        let keepFetching = true;

        console.log(`[${identifier}] Sync started...`);

        while (keepFetching) {
            // Лимит Vercel Free (10 сек) или Pro (60 сек). 
            // Ставим защиту, чтобы не отвалилось по тайм-ауту с ошибкой.
            if (syncedPages > 15) break; 

            const data = await getAuthorData(identifier, currentPage, delayMs);
            if (data.status !== 'success' || !data.data.poems?.length) break;

            const fetchedIds = data.data.poems.map(p => p.id);
            const existingIds = await checkExistingIds(fetchedIds);

            // Если это не 1 страница и ВСЕ стихи уже есть - стоп.
            if (currentPage > 1 && existingIds.length === fetchedIds.length) {
                keepFetching = false;
            } else {
                // Если есть новые - сохраняем
                if (existingIds.length !== fetchedIds.length) {
                    await savePageToDB(identifier, data.data);
                    syncedPages++;
                }
                currentPage++;
                await new Promise(r => setTimeout(r, delayMs));
            }
        }
        
        return res.json({ status: 'success', pagesSynced: syncedPages });

    } catch (e) {
        console.error(e);
        return res.status(500).json({ status: 'error', message: e.message });
    } finally {
        activeSyncs.delete(identifier);
    }
});

// --- DB QUERY HELPERS ---
async function getFilteredPoemsFromDB(identifier, q) {
    const client = await pool.connect();
    try {
        let sql = `SELECT * FROM poems WHERE author_identifier = $1`;
        let vals = [identifier], idx = 2;

        if (q.rubric && q.rubric !== 'all') { sql += ` AND rubric_name = $${idx++}`; vals.push(q.rubric); }
        if (q.year && q.year !== 'all') { sql += ` AND created_str LIKE $${idx++}`; vals.push(`%${q.year}%`); }
        if (q.collection && q.collection !== 'all') {
             if (q.collection === 'Без сборника') sql += ` AND (metadata->>'collection' IS NULL OR metadata->>'collection' = 'null')`;
             else { sql += ` AND metadata->>'collection' = $${idx++}`; vals.push(q.collection); }
        }
        if (q.q) {
            sql += ` AND (title ILIKE $${idx} OR text ILIKE $${idx} OR rubric_name ILIKE $${idx})`;
            vals.push(`%${q.q}%`); idx++;
        }

        let orderBy = 'ORDER BY id DESC';
        if (q.sort === 'date_asc') orderBy = 'ORDER BY id ASC';
        if (q.sort === 'popular') orderBy = 'ORDER BY COALESCE((metadata->>'rating')::int, 0) DESC';
        
        sql += ` ${orderBy} LIMIT 1000`;
        
        const rows = (await client.query(sql, vals)).rows;
        const author = (await client.query(`SELECT * FROM authors WHERE identifier=$1`, [identifier])).rows[0];
        if (!author) return null;

        return {
            status: 'success',
            data: {
                username: author.username, profileUrl: author.profile_url, avatarUrl: author.avatar_url,
                description: author.description, stats: author.stats, poems: rows.map(row => ({
                    id: parseInt(row.id), title: row.title, text: row.text, created: row.created_str,
                    rubric: { name: row.rubric_name }, collection: row.metadata.collection, rating: row.metadata.rating
                }))
            }
        };
    } finally { client.release(); }
}

// --- STATIC & MISC ---
app.get('/', (req, res) => res.sendFile(path.join(process.cwd(), 'public', 'index.html')));
app.get('/stihi', (req, res) => res.sendFile(path.join(process.cwd(), 'public', 'stihi.html')));
app.get('/author/:identifier/filters', async (req, res) => res.json(await getAuthorFilters(req.params.identifier)));
app.get('/stats', async (req, res) => {
    const c = await pool.query('SELECT COUNT(*) as a FROM authors');
    res.send(`Authors: ${c.rows[0].a}`);
});

export default app;

