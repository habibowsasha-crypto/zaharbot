# Fix report - outgoing source channels

## Problem
If the Telegram account running the userbot is an admin/owner of the source channel,
Telethon can mark source posts as `message.out=True`.

The previous code ignored such messages:
- new posts were skipped;
- albums were skipped;
- reaction reposts were skipped.

This made copying from owned/admin source channels fail silently.

## Fix
Removed the `message.out` blocking from:
- NewMessage handler;
- Album handler;
- reaction repost enqueue helper.

Duplicate protection and `listen_chats` remain active.
