#!/bin/bash

TARGET_FILE="docker-compose.yml"

# Check if "docker-compose.yml" exists
if [ -f "$TARGET_FILE" ]; then
  # Start with backup number 1.
  BACKUP_NUM=1

  # Loop to find the next available backup number.
  while [ -f "$TARGET_FILE.bak.$BACKUP_NUM" ]; do
    BACKUP_NUM=$((BACKUP_NUM + 1))
  done

  # Create the backup.
  cp "$TARGET_FILE" "$TARGET_FILE.bak.$BACKUP_NUM"

  # Confirm the operation
  if [ $? -eq 0 ]; then
    echo "Backup created: $TARGET_FILE.bak.$BACKUP_NUM"
  else
    echo "Error: Failed to create backup."
    exit 1
  fi
else
  echo "File $TARGET_FILE does not exist..."
fi

sed "s|{@current_directory}|$(pwd)|g" docker-compose.template.yml > docker-compose.yml