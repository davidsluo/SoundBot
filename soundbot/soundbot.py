import asyncio
import hashlib
import json
import logging
import os
from collections import OrderedDict

import aiohttp
import discord
from motor import motor_asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

sound_dir = os.getcwd() + '/sounds'


class SoundBot(discord.Client):
    def __init__(self, config, **options):
        super().__init__(**options)

        self.token = config['token']

        self.playing = {}

        if not os.path.isdir(sound_dir):
            log.debug(f'Sound directory "{sound_dir}" not found. Creating...')
            os.mkdir(sound_dir)
        else:
            log.debug(f'Using existing sound directory "{sound_dir}". ')

        log.debug('Initializing Database.')
        database_client = motor_asyncio.AsyncIOMotorClient(config['db_uri'])
        self.database = database_client[config['db_name']]

    def run(self, *args, **kwargs):
        if len(args) == 2:
            super(SoundBot, self).run(*args, **kwargs)
        else:
            super(SoundBot, self).run(self.token, **kwargs)

    async def on_message(self, msg: discord.Message):
        if len(msg.content) <= 1:
            return

        prefix = msg.content[0].lower()
        text = msg.content.lower()[1:].split()

        if prefix == '!':
            log.debug('Received sound command.')
            speed = None
            volume = None

            for word in text[1:]:
                if word.startswith('s'):
                    try:
                        speed = int(word[1:])
                        log.debug(f'Using speed {speed}%.')
                        break
                    except ValueError:
                        log.debug(f'Could not parse speed argument {word}.')

            for word in text[1:]:
                if word.startswith('v'):
                    try:
                        log.debug(f'Using speed {volume}%.')
                        volume = int(word[1:])
                        break
                    except ValueError:
                        log.debug(f'Could not parse volume argument {word}.')

            if speed is None:
                log.debug(f'Using default speed {speed}%.')
                speed = 100
            if volume is None:
                volume = 50
                log.debug(f'Using default volume {volume}%.')

            await self.play_sound(msg, text[0], speed, volume)
        elif prefix == '+':
            log.debug('Received add sound command.')
            await self.add_sound(msg, *text)
        elif prefix == '-':
            log.debug('Received remove sound command.')
            await self.remove_sound(msg, *text)
        elif prefix == '~':
            log.debug('Received rename sound command.')
            await self.rename_sound(msg, *text)
        elif prefix == '$':
            arg = text[0]
            if arg == 'help':
                log.debug('Received help command.')
                await self.help(msg)
            elif arg == 'list':
                log.debug('Received list sounds command.')
                await self.list(msg)
            elif arg == 'stop':
                log.debug('Received stop playback command.')
                await self.stop(msg)
            elif arg == 'stat':
                log.debug('Received stat command.')
                await self.stat(msg, text[1])

    async def play_sound(self, msg: discord.Message, name: str, speed: int = 100, volume: int = 100):
        try:
            # sound = self.sounds[name]
            sound = await self.database.sounds.find_one({'name': name})
            filename = sound['filename']
        except (KeyError, TypeError):
            await self.send_message(msg.channel, f'Sound **{name}** does not exist.')
            return

        if len(msg.mentions) == 1 and msg.mention_everyone is False:
            channel = msg.mentions[0].voice_channel
        else:
            channel = msg.author.voice_channel

        if channel is None:
            await self.send_message(msg.channel, f'Invalid target voice channel.')
            return

        speed = min(speed, 200)
        speed = max(speed, 50)

        log.info(f'Playing "{name}" in server "{msg.server}", channel "{channel}" at {speed}% speed, {volume}% volume.')
        if not self.is_voice_connected(msg.server):
            client = await self.join_voice_channel(channel)
        else:
            client = self.voice_client_in(msg.server)
            client.move_to(channel)

        notifier = asyncio.Event()

        player = client.create_ffmpeg_player(f'{sound_dir}/{filename}',
                                             options=f'-filter:a "atempo={speed/100}"' if speed != 100 else None,
                                             after=lambda: notifier.set())
        player.volume = volume / 100
        player.start()

        await notifier.wait()
        await client.disconnect()
        # sound['played'] += 1
        await self.database.sounds.update_one({'name': name}, {'$inc': {'played': 1}})
        self.playing[msg.server.id] = name

    async def add_sound(self, msg: discord.Message, name: str, link: str = None):
        num_sounds = await self.database.sounds.count({'name': name})
        # Disallow duplicate names
        if num_sounds > 0:
            await self.send_message(msg.channel, f'Sound named {name} already exists.')
            return

        # Resolve download url.
        if link is None:
            try:
                link = msg.attachments[0]['url']
            except (IndexError, KeyError):
                await self.send_message(msg.channel, 'Link or attachment required.')

        # Download file
        log.debug(f'Downloading from "{link}".')
        async with aiohttp.ClientSession() as session:
            async with session.get(link) as resp:
                if resp.status == 200:

                    # Write response to temporary file and moves it to the /sounds directory when done.
                    # Filename = blake2 hash of file
                    hash = hashlib.blake2b()
                    with open(f'{os.getcwd()}/tempsound', 'wb') as f:
                        while True:
                            chunk = await resp.content.read(1024)
                            if not chunk:
                                break
                            hash.update(chunk)
                            f.write(chunk)

                    filename = hash.hexdigest().upper()
                    log.debug(f'File hash is "{filename}"')

                    try:
                        os.rename(f'{os.getcwd()}/tempsound', f'{sound_dir}/{filename}')

                        # self.sounds[name] = {'filename': filename,
                        #                      'played': 0,
                        #                      'stopped': 0}
                        sound = {'name': name,
                                 'filename': filename,
                                 'played': 0,
                                 'stopped': 0}
                        await self.database.sounds.insert_one(sound)
                        await self.send_message(msg.channel, f'Saved **{name}**.')
                        log.info(f'{msg.author.name} ({msg.server.name}) added "{name}" from "{link}".')
                    except FileExistsError:
                        await self.send_message(msg.channel, f'Sound already exists.')
                        log.debug(f'File with hash "{filename}" already exists. Aborting.')

                else:
                    await self.send_message(msg.channel, f'Error while downloading: {resp.status}: {resp.reason}.')
                    log.debug(f'Status {resp.status}: {resp.reason} from download. Aborting.')
                await resp.release()

    async def remove_sound(self, msg: discord.Message, name: str):
        sound = await self.database.sounds.find_one_and_delete({'name': name})
        if sound is None:
            await self.send_message(msg.channel, f'Sound **{name}** does not exist.')
            return

        # sound = self.sounds.pop(name)
        filename = sound['filename']
        os.remove(f'{sound_dir}/{filename}')
        await self.send_message(msg.channel, f'Removed **{name}**.')
        log.info(f'{msg.author.name} ({msg.server.name}) removed "{name}".')

    async def rename_sound(self, msg: discord.Message, name: str, new_name: str):
        sound = await self.database.sounds.find_one_and_update({'name': name}, {'$set': {'name': new_name}})
        # try:
        #     sound = self.sounds.pop(name)
        # except KeyError:
        if sound is None:
            await self.send_message(msg.channel, f'Sound **{name}** does not exist.')
            return

        # self.sounds[new_name] = sound
        await self.send_message(msg.channel, f'**{name}** renamed to **{new_name}**.')
        log.info(f'{msg.author.name} ({msg.server.name}) renamed "{name}" to "{new_name}".')

    async def help(self, msg: discord.Message):
        help_msg = (
            '```\n'
            '+------------------------+------------------------------------------+\n'
            '| Command                | Function                                 |\n'
            '+------------------------+------------------------------------------+\n'
            '| !<name> [vXX] [sYY]    | Play a sound at XX% volume and YY% speed |\n'
            '| +<name> <link>         | Add a new sound                          |\n'
            '| -<name>                | Remove a sound                           |\n'
            '| ~<name> <new_name>     | Rename a sound                           |\n'
            '| $list                  | Print a list of all sounds               |\n'
            '| $stop                  | Force stop sound playback                |\n'
            '| $stat                  | Get playback stats for a sound           |\n'
            '| $help                  | Print this message                       |\n'
            '+------------------------+------------------------------------------+\n'
            '```'
        )

        await self.send_message(msg.channel, help_msg)

    async def list(self, msg: discord.Message):
        sounds = await self.database.sounds.find().sort('name').to_list(None)
        if len(sounds) == 0:
            message = 'No sounds yet. Add one with `+<name> <link>`!'
        else:
            split = OrderedDict()
            for sound in sounds:
                name = sound['name']
                first = name[0].lower()
                if first not in 'abcdefghijklmnopqrstuvwxyz':
                    first = '#'
                if first not in split.keys():
                    split[first] = [name]
                else:
                    split[first].append(name)

            message = '**Sounds**\n'

            for letter, sounds_ in split.items():
                line = f'**`{letter}`**: {", ".join(sounds_)}\n'
                message += line

        await self.send_message(msg.channel, message)

    async def stop(self, msg: discord.Message):
        if not self.is_voice_connected(msg.server):
            return

        client = self.voice_client_in(msg.server)
        await client.disconnect()

        name = self.playing[msg.server.id]
        await self.database.sounds.update_one({'name': name}, {'$inc': {'stopped': 1}})

        log.info(f'{msg.author.name} ({msg.server.name}) stopped playback of {name}.')

    async def stat(self, msg: discord.Message, name: str):
        sound = await self.database.sounds.find_one({'name': name})

        if sound is None:
            await self.send_message(msg.channel, f'Sound **{name}** does not exist.')
            return

        played = sound['played']
        stopped = sound['stopped']

        resp = f'**{name}** stats:\nPlayed {played} times.\nStopped {stopped} times.'
        await self.send_message(msg.channel, resp)


def main():
    log.debug('Initializing Soundbot...')

    log.debug('Loading config...')
    with open('config.json', 'r') as f:
        config = json.load(f)

    bot = SoundBot(config)

    log.info('Starting bot...')
    bot.run()
