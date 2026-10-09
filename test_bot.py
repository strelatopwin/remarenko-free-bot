import importlib.util
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path


os.environ.setdefault("BOT_TOKEN", "123456:abcdefghijklmnopqrstuvwxyzABCDEFGHI")
_tmp = tempfile.TemporaryDirectory()
os.environ["DATABASE_PATH"] = str(Path(_tmp.name) / "test.sqlite3")

spec = importlib.util.spec_from_file_location("remarenko_free_bot", Path(__file__).with_name("bot.py"))
bot = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(bot)


class RecommendationTests(unittest.TestCase):
    def test_beginner_self_study_gets_basic(self):
        tariff, _ = bot.recommend_tariff("new", "200", "start", "self")
        self.assertEqual(tariff, "BASIC")

    def test_first_deals_and_group_support_gets_group(self):
        tariff, _ = bot.recommend_tariff("first", "500", "system", "group")
        self.assertEqual(tariff, "GROUP")

    def test_active_scaling_with_personal_support_gets_master(self):
        tariff, _ = bot.recommend_tariff("active", "more", "scale", "personal")
        self.assertEqual(tariff, "MASTER")


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        bot.connect().close()
        with sqlite3.connect(bot.DATABASE_PATH) as db:
            for table in ("scheduled_messages", "funnel_events", "settings", "users"):
                db.execute(f"DELETE FROM {table}")
            db.commit()

    def test_schema_and_timer_schedule_persist(self):
        with bot.connect() as db:
            db.execute(
                """
                INSERT INTO users
                    (telegram_id, source, stage, subscribed, created_at, updated_at)
                VALUES (?, 'test', 'started', 0, ?, ?)
                """,
                (1, bot.now_iso(), bot.now_iso()),
            )
            db.commit()
        bot.schedule_funnel(1)
        with bot.connect() as db:
            rows = db.execute(
                "SELECT kind FROM scheduled_messages WHERE user_id=1 ORDER BY due_at"
            ).fetchall()
        self.assertEqual(
            [row["kind"] for row in rows],
            ["lesson_reminder", "value_case", "tariff_offer", "final_cta"],
        )

    def test_test_mode_resets_all_four_timers(self):
        with bot.connect() as db:
            db.execute(
                """
                INSERT INTO users
                    (telegram_id, source, stage, subscribed, created_at, updated_at)
                VALUES (?, 'test', 'started', 0, ?, ?)
                """,
                (2, bot.now_iso(), bot.now_iso()),
            )
            db.commit()
        bot.schedule_funnel(2)
        bot.schedule_funnel(2, test_mode=True)
        with bot.connect() as db:
            count = db.execute(
                """
                SELECT COUNT(*) FROM scheduled_messages
                WHERE user_id=2 AND sent_at IS NULL AND canceled_at IS NULL
                """
            ).fetchone()[0]
        self.assertEqual(count, 4)


if __name__ == "__main__":
    unittest.main()
