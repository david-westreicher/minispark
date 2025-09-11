from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings, KeyPressEvent

from .execution import PythonExecutionEngine
from .parser import parse_sql

kb = KeyBindings()


@kb.add("enter")
def _(event: KeyPressEvent) -> None:
    if event.current_buffer.text.strip()[-1:] == ";":
        event.current_buffer.validate_and_handle()
    else:
        event.current_buffer.insert_text("\n")


# History file
history = FileHistory("sql_history.txt")
session: PromptSession[str] = PromptSession(
    history=history,
    auto_suggest=AutoSuggestFromHistory(),
    multiline=True,
    key_bindings=kb,
)

BEGIN_PROMPT = r"""
         _      _                   __
  __ _  (_)__  (_)__ ___  ___ _____/ /__
 /  ' \/ / _ \/ (_-</ _ \/ _ `/ __/  '_/
/_/_/_/_/_//_/_/___/ .__/\_,_/_/ /_/\_\
                  /_/              v0.1
"""

print(BEGIN_PROMPT)  # noqa: T201

while True:
    try:
        # Prompt user
        query = session.prompt("mini> ")

        if query.lower() in ("exit;", "quit;"):
            break

        # For now, just echo
        df = parse_sql(query)
        with PythonExecutionEngine() as engine:
            df.engine = engine
            df.show()

    except KeyboardInterrupt:
        # Ctrl-C clears input
        continue
    except EOFError:
        # Ctrl-D exits
        break

print("Goodbye!")  # noqa: T201
