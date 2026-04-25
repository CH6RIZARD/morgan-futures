"""Gunicorn hooks — worker process does not run ``if __name__ == "__main__"``."""


def post_worker_init(worker):
    from bot import server as srv

    srv.start_background_tasks()
