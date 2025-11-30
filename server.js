import express from 'express';
import cors from 'cors';
import swaggerUi from 'swagger-ui-express';
import YAML from 'yamljs';
import path from 'path';
import { fileURLToPath } from 'url';
import pg from 'pg';
import { 
    getAuthorData, 
    getAuthorFilters, 
    getPoemById,
    getRecommendedAuthors,
    getPromoPoems,
    getWeeklyRatedAuthors,
    getActiveAuthors
} from 'stihirus-reader';

const { Pool } = pg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = process.env.PORT || 30010;

app.use(cors());

// --- GLOBAL SYNC TRACKER ---
const activeSyncs = new Map();

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

async function savePageToDB(identifier, data) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        
        const extendedStats = {
            ...data.stats,
            headerUrl: data.headerUrl,
            status: data.status,
            lastVisit: data.lastVisit,
            isPremium: data.isPremium
        };

        await client.query(`
            INSERT INTO authors (identifier, username, real_name, profile_url, avatar_url, description, stats, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (identifier) DO UPDATE 
            SET username=EXCLUDED.username, stats=EXCLUDED.stats, updated_at=NOW();
        `, [identifier, data.username, data.canonicalUsername, data.profileUrl, data.avatarUrl, data.description, JSON.stringify(extendedStats)]);

        if (data.poems && data.poems.length > 0) {
            for (const poem of data.poems) {
                const meta = {
                    collection: poem.collection,
                    rating: poem.rating,
                    commentsCount: poem.commentsCount,
                    imageUrl: poem.imageUrl,
                    hasCertificate: poem.hasCertificate,
                    rubricUrl: poem.rubric?.url,
                    gifts: poem.gifts,
                    uniquenessStatus: poem.uniquenessStatus,
                    contest: poem.contest,
                    holidaySection: poem.holidaySection
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
        console.error("Save Error:", e);
    } finally {
        client.release();
    }
}

async function checkExistingIds(ids) {
    if (!ids || ids.length === 0) return [];
    const query = `SELECT id FROM poems WHERE id = ANY($1::bigint[])`;
    const res = await pool.query(query, [ids]);
    return res.rows.map(r => parseInt(r.id));
}

// --- SYNC LOGIC ---

// startPage: 1 means "Start checking from the beginning". 
// If called from Homepage handler, we might pass 2 because page 1 is already handled.
async function syncAuthorContent(identifier, delayMs = 500, startPage = 1) {
    console.log(`[SYNC] Starting sync for ${identifier} from page ${startPage}...`);
    try {
        let currentPage = startPage;
        let keepFetching = true;
        
        // Safety break
        const MAX_PAGES_SYNC = 50; 

        while (keepFetching && currentPage < MAX_PAGES_SYNC) {
            const pageRes = await getAuthorData(identifier, currentPage, delayMs);
            
            if (pageRes.status !== 'success' || !pageRes.data.poems || !pageRes.data.poems.length) {
                console.log(`[SYNC] Page ${currentPage} empty or error. Stopping.`);
                break;
            }
            
            const pIds = pageRes.data.poems.map(p => p.id);
            const eIds = await checkExistingIds(pIds);
            
            await savePageToDB(identifier, pageRes.data);
            
            // If ALL poems on this page already exist in DB, we assume we reached old data
            if (eIds.length === pIds.length && pIds.length > 0) {
                console.log(`[SYNC] All poems on page ${currentPage} exist. Sync finished.`);
                keepFetching = false;
            } else {
                console.log(`[SYNC] Page ${currentPage} had new content. Continuing...`);
                currentPage++;
            }
        }
    } catch (e) {
        console.error(`[SYNC] Error syncing ${identifier}:`, e);
    }
}

// --- CORE SEARCH & FILTER & SORT ---
async function getFilteredPoemsFromDB(identifier, queryParams) {
    const client = await pool.connect();
    try {
        let sql = `SELECT * FROM poems WHERE author_identifier = $1`;
        let values = [identifier];
        let paramIndex = 2;

        const { q, searchFields, sort, year, rubric, collection } = queryParams;

        // 1. FILTERS
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

        // 2. SEARCH (Text)
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

        // 3. SORTING
        let orderBy = `ORDER BY id DESC`; 

        if (sort) {
            switch (sort) {
                case 'date_asc':
                    orderBy = `ORDER BY id ASC`;
                    break;
                case 'date_desc':
                    orderBy = `ORDER BY id DESC`;
                    break;
                case 'popular':
                case 'rating_desc':
                    orderBy = `ORDER BY COALESCE((metadata->>'rating')::int, 0) DESC, id DESC`;
                    break;
                case 'rating_asc':
                    orderBy = `ORDER BY COALESCE((metadata->>'rating')::int, 0) ASC, id DESC`;
                    break;
                case 'title_asc':
                    orderBy = `ORDER BY title ASC`;
                    break;
                case 'title_desc':
                    orderBy = `ORDER BY title DESC`;
                    break;
                default:
                    orderBy = `ORDER BY id DESC`;
            }
        }

        sql += ` ${orderBy}`;

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
            hasCertificate: row.metadata.hasCertificate,
            gifts: row.metadata.gifts || [],
            uniquenessStatus: row.metadata.uniquenessStatus,
            contest: row.metadata.contest || null,
            holidaySection: row.metadata.holidaySection || null
        }));

        const dbStats = author.stats || {};
        const { headerUrl, status, lastVisit, isPremium, ...baseStats } = dbStats;

        return {
            status: 'success',
            data: {
                authorId: 0,
                username: author.username,
                canonicalUsername: author.username,
                profileUrl: author.profile_url,
                avatarUrl: author.avatar_url,
                description: author.description,
                headerUrl: headerUrl || null,
                status: status || '',
                lastVisit: lastVisit || '',
                isPremium: !!isPremium,
                stats: baseStats,
                collections: [],
                poems: poems
            }
        };

    } finally {
        client.release();
    }
}


// --- ROUTES ---

app.get('/', (req, res) => {
    const indexPath = path.join(process.cwd(), 'public', 'index.html');
    res.sendFile(indexPath);
});

app.get('/stihi', (req, res) => {
    const stihiPath = path.join(process.cwd(), 'public', 'stihi.html');
    res.sendFile(stihiPath);
});

app.get('/robots.txt', (req, res) => {
    res.type('text/plain');
    res.send(`User-agent: *\nAllow: /$\nAllow: /stihi\nAllow: /docs\nDisallow: /`);
});

app.get('/logs', async (req, res) => {
    try {
        const result = await pool.query(`SELECT * FROM request_logs ORDER BY id DESC LIMIT 500`);
        res.type('text/plain');
        let output = "--- SERVER LOGS (Last 500) ---\nFormat: [Time] [Method] [Status] [Duration] IP Path\n\n";
        result.rows.forEach(row => {
            const date = new Date(row.created_at).toISOString();
            output += `[${date}] [${row.method}] [${row.status}] [${row.duration_ms}ms] ${row.ip} ${row.path}\n`;
        });
        res.send(output);
    } catch (e) { res.status(500).send("Log Error"); }
});

app.get('/stats', async (req, res) => {
    try {
        const client = await pool.connect();
        const count = async (t) => (await client.query(`SELECT COUNT(*) FROM ${t}`)).rows[0].count;
        const countSince = async (i) => (await client.query(`SELECT COUNT(*) FROM request_logs WHERE created_at > NOW() - INTERVAL '${i}'`)).rows[0].count;
        
        const [totalReq, week, month, year, authors, poems, dbSize] = await Promise.all([
            count('request_logs'), countSince('1 week'), countSince('1 month'), countSince('1 year'),
            count('authors'), count('poems'), client.query("SELECT pg_size_pretty(pg_database_size(current_database())) as size")
        ]);
        const avg = (await client.query('SELECT AVG(duration_ms) as avg FROM request_logs')).rows[0].avg;
        
        let out = `--- STATS ---\nRequests (Total): ${totalReq}\nRequests (7d): ${week}\nRequests (30d): ${month}\nAvg Latency: ${Math.round(avg)}ms\n`;
        out += `Content: ${authors} authors, ${poems} poems\nDB Size: ${dbSize.rows[0].size}\n`;
        client.release();
        res.type('text/plain').send(out);
    } catch (e) { res.status(500).send("Stats Error"); }
});

app.get('/homepage/recommended', async (req, res) => {
    try { res.json(await getRecommendedAuthors()); } catch (e) { res.status(500).json({ status: 'error', error: { code: 500, message: e.message } }); }
});

app.get('/homepage/promo', async (req, res) => {
    try { res.json(await getPromoPoems()); } catch (e) { res.status(500).json({ status: 'error', error: { code: 500, message: e.message } }); }
});

app.get('/homepage/weekly', async (req, res) => {
    try { res.json(await getWeeklyRatedAuthors()); } catch (e) { res.status(500).json({ status: 'error', error: { code: 500, message: e.message } }); }
});

app.get('/homepage/active', async (req, res) => {
    try { res.json(await getActiveAuthors()); } catch (e) { res.status(500).json({ status: 'error', error: { code: 500, message: e.message } }); }
});

app.get('/poem/:id', async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) return res.status(400).json({ status: 'error', error: { code: 400, message: 'Invalid ID' } });
    try { res.json(await getPoemById(id)); } catch (e) { res.status(500).json({ status: 'error', error: { code: 500, message: e.message } }); }
});

// --- MAIN AUTHOR ENDPOINT ---
app.get('/author/:identifier', async (req, res) => {
    const identifier = req.params.identifier;
    let page = req.query.page;
    let delayMs = req.query.delay ? parseInt(req.query.delay) : 500;
    
    // Page logic: If undefined or empty, treat as 1.
    const pageNum = (page !== undefined && page !== 'null' && page !== '') ? parseInt(page, 10) : 1;

    // Filter Logic Check
    const hasFilters = req.query.q || req.query.year || (req.query.rubric && req.query.rubric !== 'all') || (req.query.collection && req.query.collection !== 'all');

    try {
        // CASE A: FILTERS ARE ACTIVE
        if (hasFilters) {
            // 1. If sync is running, wait for it.
            if (activeSyncs.has(identifier)) {
                console.log(`[WAIT] Request for ${identifier} waiting for sync...`);
                await activeSyncs.get(identifier); 
            }
            // 2. If NO sync, START it and wait (ensures we search fresh data)
            else {
                 console.log(`[SYNC] Triggering sync before search for ${identifier}...`);
                 const syncPromise = syncAuthorContent(identifier, delayMs, 1);
                 activeSyncs.set(identifier, syncPromise);
                 try {
                     await syncPromise;
                 } finally {
                     activeSyncs.delete(identifier);
                 }
            }

            // 3. Search DB
            const finalResponse = await getFilteredPoemsFromDB(identifier, req.query);
            if (finalResponse) return res.json(finalResponse);
            return res.status(404).json({ status: 'error', error: { code: 404, message: 'Author not found' } });
        }

        // CASE B: PAGINATION (Page > 1)
        if (pageNum > 1) {
            const freshData = await getAuthorData(identifier, pageNum, delayMs);
            if (freshData.status === 'success') {
                await savePageToDB(identifier, freshData.data);
                return res.json(freshData);
            } else {
                return res.status(freshData.error?.code || 500).json(freshData);
            }
        }

        // CASE C: HOMEPAGE (Page 1) - Smart Sync Trigger
        if (pageNum === 1) {
            const freshData = await getAuthorData(identifier, 1, delayMs);
            
            if (freshData.status === 'success') {
                // 1. Check for new content BEFORE saving
                // If we save first, the checkExistingIds will say "all exist"
                const fetchedIds = (freshData.data.poems || []).map(p => p.id);
                const existingIds = await checkExistingIds(fetchedIds);
                const hasNewContent = existingIds.length !== fetchedIds.length;

                // 2. Save Page 1
                await savePageToDB(identifier, freshData.data);
                
                // 3. Return Page 1 to Client IMMEDIATELY
                res.json(freshData);

                // 4. TRIGGER BACKGROUND SYNC IF NEW CONTENT FOUND
                if (hasNewContent && !activeSyncs.has(identifier)) {
                    console.log(`[BG] New poems detected for ${identifier}. Starting deep sync from page 2...`);
                    // Start from page 2 because we just saved page 1
                    const syncPromise = syncAuthorContent(identifier, delayMs, 2);
                    activeSyncs.set(identifier, syncPromise);
                    syncPromise.finally(() => {
                        console.log(`[BG] Sync finished for ${identifier}`);
                        activeSyncs.delete(identifier);
                    });
                }
            } else {
                return res.status(freshData.error?.code || 500).json(freshData);
            }
        }

    } catch (err) {
        console.error(err);
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
