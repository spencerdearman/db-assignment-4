
# Delete the old file
rm analytics.db

# Re-create the empty tables
npx ts-node src/index.ts init

# Run the new load
npx ts-node src/index.ts full-load