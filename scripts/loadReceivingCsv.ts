import fs from 'fs';
import path from 'path';
import { Client } from 'pg';
import csv from 'csv-parser';
import 'dotenv/config';

const client = new Client();

const tableName = 'receiving_stats';

async function createTableIfNotExists() {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id SERIAL PRIMARY KEY,
      player TEXT,
      age INT,
      team TEXT,
      pos TEXT,
      games INT,
      games_started INT,
      targets INT,
      receptions INT,
      yards INT,
      touchdowns INT,
      yards_per_reception FLOAT,
      longest INT,
      receptions_per_game FLOAT,
      yards_per_game FLOAT,
      catch_pct FLOAT,
      yards_per_target FLOAT,
      first_downs INT,
      success_pct FLOAT,
      fumbles INT,
      awards TEXT,
      season INT
    );
  `);
}

async function loadCSV(filePath: string) {
  return new Promise<void>((resolve, reject) => {
    const season = parseInt(filePath.match(/\d{4}/)?.[0] || '0');
    const rows: any[] = [];

    fs.createReadStream(filePath)
      .pipe(csv({ skipLines: 1 }))
      .on('data', async (row) => {
        if (!row['Player'] || row['Player'] === 'Player') return;

        const cleaned = {
          player: row['Player']?.trim(),
          age: parseInt(row['Age'] || '0'),
          team: (row['Tm']?.trim() || row['Team']?.trim() || '').toUpperCase(),
          pos: row['FantPos']?.trim() || row['Pos']?.trim(),
          games: parseInt(row['G'] || '0'),
          games_started: parseInt(row['GS'] || '0'),
          targets: parseInt(row['Tgt'] || '0'),
          receptions: parseInt(row['Rec'] || '0'),
          yards: parseInt(row['Yds'] || '0'),
          touchdowns: parseInt(row['TD'] || '0'),
          yards_per_reception: parseFloat(row['Y/R'] || '0'),
          longest: parseInt(row['Lng'] || '0'),
          receptions_per_game: parseFloat(row['R/G'] || '0'),
          yards_per_game: parseFloat(row['Y/G'] || '0'),
          catch_pct: parseFloat(row['Ctch%'] || '0'),
          yards_per_target: parseFloat(row['Y/Tgt'] || '0'),
          first_downs: parseInt(row['1D'] || '0'),
          success_pct: parseFloat(row['Succ%'] || '0'),
          fumbles: parseInt(row['Fmb'] || '0'),
          awards: row['Awards']?.trim(),
          season,
        };

        rows.push(cleaned);
      })
      .on('end', async () => {
        await createTableIfNotExists();
        for (const r of rows) {
          await client.query(
            `INSERT INTO ${tableName} (
              player, age, team, pos, games, games_started, targets, receptions, yards,
              touchdowns, yards_per_reception, longest, receptions_per_game, yards_per_game,
              catch_pct, yards_per_target, first_downs, success_pct, fumbles, awards,
              season)
             VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9,
              $10, $11, $12, $13, $14,
              $15, $16, $17, $18, $19, $20,
              $21);
            `,
            [
              r.player, r.age, r.team, r.pos, r.games, r.games_started, r.targets,
              r.receptions, r.yards, r.touchdowns, r.yards_per_reception, r.longest,
              r.receptions_per_game, r.yards_per_game, r.catch_pct, r.yards_per_target,
              r.first_downs, r.success_pct, r.fumbles, r.awards, r.season
            ]
          );
        }
        console.log(`✅ Loaded ${rows.length} rows from ${path.basename(filePath)} into ${tableName}`);
        resolve();
      })
      .on('error', reject);
  });
}

async function main() {
  const dataDir = './data/receiving';
  const files = fs.readdirSync(dataDir).filter(file => file.endsWith('.csv')).map(file => path.join(dataDir, file));

  if (files.length === 0) {
    console.error('❌ No CSV files found in data/receiving');
    process.exit(1);
  }

  await client.connect();

  for (const file of files) {
    await loadCSV(file);
  }

  await client.end();
  console.log('🏁 All done');
}

main().catch(err => {
  console.error('❌ Error during CSV load:', err);
  client.end();
});
