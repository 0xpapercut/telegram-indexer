from telethon import types

async def async_enumerate(aiterable, start=0):
    index = start
    async for item in aiterable:
        yield index, item
        index += 1

def get_username(user: types.User):
    if username := user.username:
        return username
    if user.usernames:
        for username in user.usernames:
            return username.username

def get_full_name(user: types.User):
    names = []
    if user.first_name:
        names.append(user.first_name)
    if username := get_username(user):
        names.append(f"'{username}'")
    if user.last_name:
        names.append(user.last_name)
    return ' '.join(names)
