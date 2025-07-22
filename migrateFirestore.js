// migrateFirestore.js

import admin from 'firebase-admin';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { parse as csvParse } from 'csv-parse/sync';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- Firebase Admin SDK Initialization ---
// Make sure your serviceAccountKey.json is in the same directory as this script.
const serviceAccountPath = path.resolve(__dirname, 'serviceAccountKey.json');

let serviceAccount;
try {
  serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, 'utf8'));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    // If you were also using Realtime Database, you might specify databaseURL here.
    // For Firestore alone, it's often not strictly necessary if project ID is in the service account.
  });
  console.log('Firebase Admin SDK initialized successfully.');
} catch (error) {
  console.error('Error initializing Firebase Admin SDK. Make sure serviceAccountKey.json is valid and present.');
  console.error(error);
  process.exit(1); // Exit if initialization fails
}

const db = admin.firestore();

// --- Main Migration Function ---
async function importJsonToFirestore(jsonFilePath, targetCollectionName) {
  try {
    const fullCsvPath = path.resolve(__dirname, jsonFilePath);
    const rawData = fs.readFileSync(fullCsvPath, 'utf8');
    // Parse CSV using csv-parse
    const items = csvParse(rawData, {
      columns: true,
      skip_empty_lines: true
    });

    console.log(`\n--- Starting data migration ---`);
    console.log(`Source file: ${jsonFilePath}`);
    console.log(`Target collection: "${targetCollectionName}"`);
    console.log(`Total records to import: ${items.length}`);

    // Firestore allows up to 500 operations (writes, updates, deletes) in a single batch.
    // We'll process items in batches for efficiency.
    const batchSize = 400; // Keep it slightly below 500 for safety margin
    let recordsProcessed = 0;

    for (let i = 0; i < items.length; i += batchSize) {
      const batch = db.batch();
      const currentBatchItems = items.slice(i, i + batchSize);

      for (const item of currentBatchItems) {
        // --- Data Transformation Logic ---
        // Convert DynamoDB attribute format to Firestore format
        // Example: { userId: '{"S":"abc"}', createdAt: '{"S":"2025-01-01T00:00:00Z"}' }
        const firestoreDocData = {};
        for (const [key, value] of Object.entries(item)) {
          if (!value) continue;
          let parsed;
          try {
            parsed = JSON.parse(value);
          } catch {
            parsed = value;
          }
          // DynamoDB export format: { S: 'string' }, { N: '123' }, etc.
          if (parsed && typeof parsed === 'object' && (parsed.S || parsed.N || parsed.BOOL)) {
            if (parsed.S !== undefined) firestoreDocData[key] = parsed.S;
            else if (parsed.N !== undefined) firestoreDocData[key] = Number(parsed.N);
            else if (parsed.BOOL !== undefined) firestoreDocData[key] = parsed.BOOL;
            else firestoreDocData[key] = parsed;
          } else {
            firestoreDocData[key] = parsed;
          }
        }
        // Example: convert signupDate string to Firestore Timestamp
        if (firestoreDocData.signupDate && typeof firestoreDocData.signupDate === 'string') {
          try {
            firestoreDocData.signupDate = admin.firestore.Timestamp.fromDate(new Date(firestoreDocData.signupDate));
          } catch (dateError) {
            console.warn(`Could not convert signupDate: ${firestoreDocData.signupDate}`, dateError);
          }
        }
        // Use auto-generated document ID
        const docRef = db.collection(targetCollectionName).doc();
        batch.set(docRef, firestoreDocData);
      }
      await batch.commit();
      recordsProcessed += currentBatchItems.length;
      console.log(`Processed batch: ${recordsProcessed}/${items.length} records...`);
    }

    console.log(`\n--- Migration complete! Successfully imported ${recordsProcessed} records into "${targetCollectionName}" collection. ---`);
  } catch (error) {
    console.error('\n--- Error during migration ---');
    console.error(error);
  }
}

// --- Execution ---
// Replace 'dynamodb-export.csv' with the path to your actual CSV export.
// Replace 'your_new_collection_name' with the desired name for your Firestore collection.
const sourceFile = 'results.csv'; // Updated to reflect CSV file
const destinationCollection = 'ProdWebSocketConnections'; // Let's use 'users' as our example collection name

importJsonToFirestore(sourceFile, destinationCollection);

