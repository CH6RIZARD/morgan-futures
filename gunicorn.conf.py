"""Gunicorn hooks — worker process does not run ``if __name__ == "__main__"``."""

import os

# Avoid relying on shell expansion in startCommand. Railway always provides PORT.
bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"


def post_worker_init(worker):
    from bot import server as srv

    srv.start_background_tasks()
