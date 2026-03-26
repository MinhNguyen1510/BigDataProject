#!/bin/bash

# Patch Flask-Session bug: offset-naive vs offset-aware datetime
FLASK_SESSION_FILE="/home/airflow/.local/lib/python3.8/site-packages/flask_session/sessions.py"

if [ -f "$FLASK_SESSION_FILE" ]; then
    # Backup original file
    cp "$FLASK_SESSION_FILE" "$FLASK_SESSION_FILE.bak"
    
    # Patch the file - replace datetime.utcnow() with timezone-aware version
    sed -i 's|datetime.utcnow()|datetime.now(timezone.utc)|g' "$FLASK_SESSION_FILE"
    
    # Add timezone import if not exists
    if ! grep -q "from datetime import timezone" "$FLASK_SESSION_FILE"; then
        sed -i '1i from datetime import timezone' "$FLASK_SESSION_FILE"
    fi
    
    echo "✓ Flask-Session patched successfully"
else
    echo "⚠ Flask-Session file not found yet, will patch at startup"
fi
