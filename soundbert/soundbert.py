import asyncio
import logging
import platform

import asyncpg
from discord import Message, Guild
from discord.ext import commands
from discord.ext.commands import ExtensionNotFound

from .cogs.utils.reactions import no

__all__ = ['SoundBert']

log = logging.getLogger(__name__)


async def get_prefix(bot: 'SoundBert', msg: Message):
    default_prefix = bot.config['bot']['default_prefix']
    async with bot.pool.acquire() as conn:
        prefix = await conn.fetchval('SELECT prefix FROM guilds WHERE id = $1', msg.guild.id)
    return commands.when_mentioned_or(prefix if prefix else default_prefix)(bot, msg)


class SoundBert(commands.Bot):
    def __init__(self, config):
        self._ensure_event_loop()
        super().__init__(command_prefix=get_prefix)

        self.config = config
        self.pool: asyncpg.pool.Pool = self.loop.run_until_complete(asyncpg.create_pool(config['bot']['db_uri']))

        base_extensions = [
            'soundbert.cogs.soundboard',
            'soundbert.cogs.info',
            'soundbert.cogs.settings',
            'soundbert.cogs.admin'
        ]

        log.info('Loading base extensions.')
        for ext in base_extensions:
            self.load_extension(ext)
            log.debug(f'Loaded {ext}')

        log.info('Loading extra extensions.')
        for ext in config['bot']['extra_cogs']:
            try:
                self.load_extension(ext)
                log.debug(f'Loaded {ext}.')
            except ExtensionNotFound:
                log.exception(f'Failed to load {ext}')

    @staticmethod
    def _ensure_event_loop():
        if platform.system() == 'Windows':
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)
        else:
            try:
                import uvloop

                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            except ImportError:
                pass

    async def on_command_error(self, ctx: commands.Context, exception: commands.CommandError):
        log_msg = (
            f'In guild {ctx.guild.name}, channel {ctx.channel.name}, '
            f'{ctx.author.name} executed {ctx.message.content}, but encountered exception: {exception}'
        )
        if not isinstance(exception, commands.UserInputError):
            log.exception(log_msg, exc_info=exception)
        else:
            log.debug(log_msg)

        await no(ctx)
        if len(exception.args) > 0:
            msg = await ctx.send(exception.args[0])
            try:
                delay = int(exception.args[1])
            except (IndexError, ValueError):
                delay = 60
            await msg.delete(delay=delay)

    async def on_command(self, ctx: commands.Context):
        log.info(
            f'In guild {ctx.guild.name}, channel {ctx.channel.name}, '
            f'{ctx.author.name} executed {ctx.message.content}'
        )

    async def on_guild_join(self, guild: Guild):
        log.info(f'Joined guild {guild.name} ({guild.id}).')
        async with self.pool.acquire() as conn:
            await conn.execute('INSERT INTO guilds(id) VALUES ($1)', guild.id)
