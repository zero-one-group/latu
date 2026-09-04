# Loaded by `iex -S mix` in this repo — the three lines every Latu file starts with, so a REPL
# session is one `Latu.connect/1` away from a DataFrame.
#
# Deliberately does no IO and opens no connection. A `.iex.exs` that reaches for a server makes
# `iex -S mix` fail when there is no server, which is most of the time.
#
# Copy it into your own project to get the same REPL: this file is Latu's, and a dependency's
# `.iex.exs` is not loaded for you.

import Latu.Column
alias Latu.Functions, as: F
alias Latu.Window, as: W

alias Latu.{Catalog, DataFrame, Error, Plan, Progress, Session}
