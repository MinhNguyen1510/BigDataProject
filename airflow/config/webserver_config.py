# Airflow Webserver Configuration to fix Flask-Session timezone issues

from datetime import timedelta

# Session configuration - Fix for offset-naive vs offset-aware datetime issue
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
PERMANENT_SESSION_LIFETIME = timedelta(days=30)

# Use UTC for session timestamps
SESSION_TIMEZONE = 'UTC'
