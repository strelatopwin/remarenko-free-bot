import importlib.util
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from aiogram.fsm.storage.base import StorageKey
from aiogram import Bot, Dispatcher
from aiogram.client.session.base import BaseSession
from aiogram.types import Chat, Message, Update, User, CallbackQuery
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
            for table in ("scheduled_messages", "funnel_events", "settings", "users", "form_state", "lead_notifications", "channel_joins"):
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

    def test_reopening_lesson_does_not_regress_progress(self):
        with bot.connect() as db:
            db.execute("INSERT INTO users(telegram_id,stage,created_at,updated_at) VALUES(7,'recommendation_shown',?,?)", (bot.now_iso(),bot.now_iso()))
        bot.update_user(7, stage='lesson_video_sent', subscribed=1)
        self.assertEqual(bot.get_user(7)['stage'], 'recommendation_shown')
        self.assertTrue(bot.should_skip_scheduled('tariff_offer', bot.get_user(7)))
        self.assertTrue(bot.should_skip_scheduled('lesson_reminder', bot.get_user(7)))

    def test_application_is_stored_with_durable_notification(self):
        with bot.connect() as db:
            db.execute("INSERT INTO users(telegram_id,created_at,updated_at) VALUES(8,?,?)", (bot.now_iso(),bot.now_iso()))
        bot.schedule_funnel(8)
        bot.store_application(8, '@student', 'Lead text')
        self.assertEqual(bot.get_user(8)['lead_status'], 'new')
        with bot.connect() as db:
            self.assertEqual(db.execute('SELECT text FROM lead_notifications WHERE user_id=8').fetchone()[0], 'Lead text')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM scheduled_messages WHERE user_id=8 AND canceled_at IS NULL').fetchone()[0], 0)

    def test_three_form_reminders_do_not_restart_on_repeat_click(self):
        bot.schedule_form_reminder(90)
        with bot.connect() as db:
            before=[tuple(r) for r in db.execute('SELECT kind,due_at,created_at FROM scheduled_messages WHERE user_id=90 ORDER BY due_at')]
        bot.schedule_form_reminder(90)
        with bot.connect() as db:
            after=[tuple(r) for r in db.execute('SELECT kind,due_at,created_at FROM scheduled_messages WHERE user_id=90 ORDER BY due_at')]
        self.assertEqual(before,after)
        self.assertEqual(len(before),3)
        hours=[round((datetime.fromisoformat(r[1])-datetime.fromisoformat(r[2])).total_seconds()/3600) for r in before]
        self.assertEqual(hours,[2,24,48])

    def test_overdue_form_reminders_are_not_sent_in_a_burst(self):
        bot.schedule_form_reminder(90)
        with bot.connect() as db:
            for index,kind in enumerate(('form_reminder','form_reminder_24','form_reminder_48')):
                db.execute('UPDATE scheduled_messages SET due_at=? WHERE user_id=90 AND kind=?',((bot.utc_now()-timedelta(hours=3-index)).isoformat(),kind))
        bot.collapse_overdue_form_reminders()
        with bot.connect() as db:
            rows=db.execute('SELECT kind FROM scheduled_messages WHERE user_id=90 AND canceled_at IS NULL').fetchall()
        self.assertEqual([r['kind'] for r in rows],['form_reminder_48'])


class AsyncRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot.connect().close()
        with bot.connect() as db:
            for table in ('users','funnel_events','scheduled_messages','lead_notifications','form_state','channel_joins'):
                db.execute(f'DELETE FROM {table}')
            db.execute('INSERT INTO users(telegram_id,created_at,updated_at) VALUES(50,?,?)', (bot.now_iso(),bot.now_iso()))

    async def test_form_survives_new_storage_instance(self):
        key = StorageKey(bot_id=123456,chat_id=50,user_id=50)
        first=bot.SQLiteStorage()
        await first.set_state(key,bot.LeadForm.capital)
        await first.set_data(key,{'experience':'Новачок','experience_code':'new'})
        await first.close()
        second=bot.SQLiteStorage()
        self.assertEqual(await second.get_state(key),bot.LeadForm.capital.state)
        self.assertEqual((await second.get_data(key))['experience_code'],'new')

    async def test_failed_lead_notification_retries_and_then_is_sent(self):
        bot.store_application(50,'@user','Stored application')
        api=SimpleNamespace(send_message=AsyncMock(side_effect=RuntimeError('temporary outage')))
        await bot.deliver_lead_notifications(api)
        with bot.connect() as db:
            row=db.execute('SELECT * FROM lead_notifications WHERE user_id=50').fetchone()
            self.assertIsNone(row['sent_at'])
            self.assertEqual(row['attempts'],1)
            db.execute('UPDATE lead_notifications SET due_at=? WHERE user_id=50',(bot.now_iso(),))
        api.send_message=AsyncMock(return_value=SimpleNamespace(message_id=123))
        await bot.deliver_lead_notifications(api)
        with bot.connect() as db:
            self.assertIsNotNone(db.execute('SELECT sent_at FROM lead_notifications WHERE user_id=50').fetchone()[0])
        api.send_message.assert_awaited_once()

    async def test_fallback_link_records_issued_lesson(self):
        previous=bot.LESSON_URL
        bot.LESSON_URL='https://example.com/lesson'
        try:
            message=SimpleNamespace(chat=SimpleNamespace(id=50),answer=AsyncMock())
            await bot.send_lesson_video(message)
            self.assertIsNotNone(bot.get_user(50)['lesson_received_at'])
        finally: bot.LESSON_URL=previous

    async def test_early_lesson_done_is_rejected(self):
        callback=SimpleNamespace(from_user=SimpleNamespace(id=50),answer=AsyncMock(),message=SimpleNamespace(answer=AsyncMock()))
        await bot.lesson_done(callback)
        callback.message.answer.assert_not_awaited()

    async def test_resume_after_recommendation_asks_for_contact(self):
        bot.update_user(50,stage='recommendation_shown',recommendation='GROUP')
        state=SimpleNamespace(get_state=AsyncMock(return_value=None),set_state=AsyncMock())
        callback=SimpleNamespace(from_user=SimpleNamespace(id=50,username='student'),answer=AsyncMock(),message=SimpleNamespace(answer=AsyncMock()))
        await bot.resume_form(callback,state)
        state.set_state.assert_awaited_once_with(bot.LeadForm.contact)

    async def test_questionnaire_notifies_owner_before_contact(self):
        api=SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=777)))
        message=SimpleNamespace(chat=SimpleNamespace(id=50),answer=AsyncMock(),bot=api)
        await bot.send_recommendation(message, {'experience_code':'new','capital_code':'200','goal_code':'start','support_code':'self'})
        self.assertIsNone(bot.get_user(50)['contact'])
        api.send_message.assert_awaited_once()
        view=SimpleNamespace(answer=AsyncMock())
        await bot.show_leads(view,'new')
        self.assertIn('· 1', view.answer.call_args.args[0])

    async def test_contact_updates_existing_message_and_preserves_work_status(self):
        bot.update_user(50,recommendation='GROUP',stage='recommendation_shown')
        api=SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=888)),edit_message_text=AsyncMock())
        bot.queue_questionnaire(50)
        await bot.deliver_lead_notifications(api)
        bot.update_user(50,lead_status='work')
        bot.store_application(50,'@student','Updated application with contact')
        await bot.deliver_lead_notifications(api)
        api.send_message.assert_awaited_once()
        api.edit_message_text.assert_awaited_once()
        self.assertEqual(api.edit_message_text.call_args.kwargs['message_id'],888)
        self.assertEqual(bot.get_user(50)['lead_status'],'work')

    async def test_non_owner_cannot_open_lead_panel(self):
        callback=SimpleNamespace(from_user=SimpleNamespace(id=50),answer=AsyncMock(),message=SimpleNamespace(answer=AsyncMock()),data='adminlead:detail:50')
        await bot.admin_lead_callback(callback)
        callback.message.answer.assert_not_awaited()

    async def test_back_button_keeps_people_page_after_status_change(self):
        message=SimpleNamespace(edit_text=AsyncMock())
        callback=SimpleNamespace(from_user=SimpleNamespace(id=bot.ADMIN_CHAT_ID),answer=AsyncMock(),message=message,data='adminlead:detail:50:people:2')
        await bot.admin_lead_callback(callback)
        markup=message.edit_text.call_args.kwargs['reply_markup']
        self.assertEqual(markup.inline_keyboard[-1][0].callback_data,'adminlead:list:people:2')
        callback.data=markup.inline_keyboard[1][0].callback_data
        await bot.admin_lead_callback(callback)
        markup=message.edit_text.call_args.kwargs['reply_markup']
        self.assertEqual(markup.inline_keyboard[-1][0].callback_data,'adminlead:list:people:2')
        self.assertEqual(bot.get_user(50)['lead_status'],'work')

    async def test_list_cards_include_filter_and_page(self):
        with bot.connect() as db:
            for uid in range(60,70):
                db.execute('INSERT INTO users(telegram_id,created_at,updated_at) VALUES(?,?,?)',(uid,bot.now_iso(),bot.now_iso()))
        message=SimpleNamespace(answer=AsyncMock())
        await bot.show_leads(message,'people',1)
        markup=message.answer.call_args.kwargs['reply_markup']
        self.assertTrue(markup.inline_keyboard[0][0].callback_data.endswith(':people:1'))

    async def test_join_count_is_unique_and_identifies_invite(self):
        previous=bot.CHANNEL_ID
        bot.CHANNEL_ID='-10042'
        try:
            user=SimpleNamespace(id=50,is_bot=False)
            event=SimpleNamespace(chat=SimpleNamespace(id=-10042,username=None),date=datetime.now(timezone.utc),
                old_chat_member=SimpleNamespace(status='left'),new_chat_member=SimpleNamespace(status='member',user=user),
                invite_link=SimpleNamespace(invite_link=bot.CHANNEL_URL))
            await bot.track_channel_join(event)
            await bot.track_channel_join(event)
            with bot.connect() as db:
                count,via=db.execute('SELECT COUNT(*),SUM(via_bot_link) FROM channel_joins').fetchone()
            self.assertEqual((count,via),(1,1))
        finally: bot.CHANNEL_ID=previous

    async def test_full_questionnaire_through_real_dispatcher(self):
        class FakeSession(BaseSession):
            def __init__(self):
                super().__init__()
                self.sent=[]
            async def close(self): pass
            async def make_request(self, api, method, timeout=None):
                if hasattr(method, 'chat_id') and hasattr(method, 'text'):
                    self.sent.append(method.text)
                    return Message(message_id=len(self.sent),date=datetime.now(timezone.utc),chat=Chat(id=int(method.chat_id),type='private'),text=method.text)
                return True
            async def stream_content(self, *args, **kwargs):
                if False: yield b''
        session=FakeSession()
        api=Bot(os.environ['BOT_TOKEN'],session=session)
        dp=Dispatcher(storage=bot.SQLiteStorage())
        dp.include_router(bot.router)
        uid=100
        async def send_text(text, who=uid):
            msg=Message(message_id=len(session.sent)+1,date=datetime.now(timezone.utc),chat=Chat(id=who,type='private'),from_user=User(id=who,is_bot=False,first_name='Student'),text=text)
            await dp.feed_update(api,Update(update_id=len(session.sent)+1,message=msg))
        async def press(data):
            msg=Message(message_id=1,date=datetime.now(timezone.utc),chat=Chat(id=uid,type='private'),from_user=User(id=123456,is_bot=True,first_name='Bot'),text='menu')
            cb=CallbackQuery(id='query',from_user=User(id=uid,is_bot=False,first_name='Student'),chat_instance='chat',message=msg,data=data)
            await dp.feed_update(api,Update(update_id=len(session.sent)+1,callback_query=cb))
        await send_text('/start instagram')
        for data in ('apply','exp:first','cap:500','goal:system','sup:group','contact_start'):
            await press(data)
        await send_text('@student')
        user=bot.get_user(uid)
        self.assertEqual(user['stage'],'application_completed')
        self.assertEqual(user['recommendation'],'GROUP')
        self.assertEqual(user['source'],'instagram')
        self.assertEqual(user['contact'],'@student')
        await send_text('/leads',bot.ADMIN_CHAT_ID)
        self.assertTrue(any('Нові' in text for text in session.sent))
        await dp.storage.close()
        await api.session.close()


if __name__ == "__main__":
    unittest.main()
