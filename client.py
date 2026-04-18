# -*- coding: utf-8 -*-
"""
Mobile Money Wallet — Terminal Client
Connects to the Distributed Ledger System API.
"""

import base64
import uuid
import sys
from decimal import Decimal, InvalidOperation

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.text import Text
from rich.rule import Rule
from rich import box
from rich.align import Align
from rich.columns import Columns
from rich.live import Live
from rich.spinner import Spinner
import time
import io
import os

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_URL = "http://localhost:8000"
USERNAME = "admin"
PASSWORD = "password123"
CURRENCY = "UGX"

console = Console(force_terminal=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def auth_header() -> dict:
    token = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def fmt_money(amount) -> str:
    try:
        val = Decimal(str(amount))
        return f"{CURRENCY} {val:,.2f}"
    except Exception:
        return str(amount)


def api_get(path: str) -> dict | None:
    try:
        r = requests.get(f"{BASE_URL}{path}", headers=auth_header(), timeout=5)
        return r.json()
    except requests.ConnectionError:
        console.print("[bold red]✗ Cannot reach server. Is it running on localhost:8000?[/]")
        return None
    except Exception as e:
        console.print(f"[red]✗ Error: {e}[/]")
        return None


def api_post(path: str, payload: dict) -> tuple[bool, dict]:
    try:
        r = requests.post(f"{BASE_URL}{path}", json=payload, headers=auth_header(), timeout=10)
        data = r.json()
        return r.status_code in (200, 201), data
    except requests.ConnectionError:
        console.print("[bold red]✗ Cannot reach server. Is it running on localhost:8000?[/]")
        return False, {}
    except Exception as e:
        console.print(f"[red]✗ Error: {e}[/]")
        return False, {}


def spinner_call(label: str, fn, *args, **kwargs):
    """Run a function with a spinner while it executes."""
    result = [None]
    with console.status(f"[cyan]{label}...[/]", spinner="dots"):
        result[0] = fn(*args, **kwargs)
    return result[0]


def clear():
    console.clear()


# ── Screens ────────────────────────────────────────────────────────────────────

def show_banner():
    title = Text("MOBILE MONEY WALLET", style="bold cyan", justify="center")
    sub   = Text("Distributed Ledger System  |  Uganda", style="dim white", justify="center")
    line  = Text("=" * 46, style="bold green", justify="center")

    console.print()
    console.print(Align.center(line))
    console.print(Align.center(title))
    console.print(Align.center(sub))
    console.print(Align.center(line))
    console.print()


def show_main_menu() -> str:
    console.print(Rule("[bold cyan]Main Menu[/]"))
    console.print()

    options = [
        ("1", "$",  "Check Balance",  "View your account balance"),
        ("2", ">>", "Send Money",     "Transfer funds to another account"),
        ("3", "i",  "Account Info",   "View account details"),
        ("4", "+",  "System Health",  "Check API server status"),
        ("5", "x",  "Exit",           "Quit the client"),
    ]

    table = Table(box=box.ROUNDED, border_style="cyan", show_header=False, padding=(0, 2))
    table.add_column("Key",  style="bold yellow",  width=5)
    table.add_column("Icon", style="bold magenta", width=4)
    table.add_column("Action", style="bold white",  width=18)
    table.add_column("Description", style="dim",   width=38)

    for key, icon, action, desc in options:
        table.add_row(f"[{key}]", icon, action, desc)

    console.print(Align.center(table))
    console.print()

    choice = Prompt.ask(
        "[bold yellow]Choose an option[/]",
        choices=["1", "2", "3", "4", "5"],
        show_choices=False,
    )
    return choice


def screen_check_balance():
    clear()
    console.print(Rule("[bold cyan] Check Balance [/]"))
    console.print()

    account_id = Prompt.ask("[cyan]Enter your Account ID[/]").strip()

    if not account_id:
        console.print("[red]Account ID cannot be empty.[/]")
        _pause()
        return

    # Hit the health endpoint first just to validate server is up
    result = spinner_call("Fetching balance", api_get, f"/api/v1/accounts/{account_id}/balance")

    if result is None:
        _pause()
        return

    if "error" in str(result.get("status", "")).lower() or "detail" in result:
        msg   = result.get("message", result.get("detail", "Unknown error"))
        code  = result.get("error_code", "")
        error_code_text = f"\n[dim]Error Code: {code}[/]" if code else ""
        console.print(Panel(
            f"[red]!! {msg}[/]{error_code_text}",
            title="[red]Error[/]", border_style="red"
        ))
    else:
        balance = result.get("balance", result.get("amount", "N/A"))
        currency = result.get("currency", CURRENCY)
        owner = result.get("owner_id", "—")

        try:
            formatted_balance = f"{Decimal(str(balance)):,.2f}"
        except InvalidOperation:
            formatted_balance = str(balance)

        table = Table(box=box.SIMPLE_HEAD, border_style="green", show_header=False)
        table.add_column("Field",  style="dim", width=18)
        table.add_column("Value",  style="bold white")

        table.add_row("Account ID",  account_id)
        table.add_row("Owner",       owner)
        table.add_row("Balance",     f"[bold green]{currency} {formatted_balance}[/]")
        table.add_row("Status",      result.get("status", "active").upper())

        console.print(Panel(table, title="[green]Account Balance[/]", border_style="green"))

    _pause()


def screen_send_money():
    clear()
    console.print(Rule("[bold cyan] Send Money [/]"))
    console.print()

    from_id = Prompt.ask("[cyan]Your Account ID (sender)[/]").strip()
    to_id   = Prompt.ask("[cyan]Recipient Account ID[/]").strip()

    # Amount input with validation
    while True:
        raw = Prompt.ask(f"[cyan]Amount ({CURRENCY})[/]").strip()
        try:
            amount = Decimal(raw)
            if amount <= 0:
                console.print("[red]Amount must be greater than 0.[/]")
                continue
            break
        except InvalidOperation:
            console.print("[red]Invalid amount. Enter a number like 5000 or 250.50[/]")

    # Show confirmation
    console.print()
    console.print(Panel(
        f"[bold]From:[/]   [cyan]{from_id}[/]\n"
        f"[bold]To:[/]     [cyan]{to_id}[/]\n"
        f"[bold]Amount:[/] [bold green]{fmt_money(amount)}[/]",
        title="[yellow]Confirm Transfer[/]",
        border_style="yellow",
    ))

    if not Confirm.ask("[yellow]Proceed with transfer?[/]"):
        console.print("[dim]Transfer cancelled.[/]")
        _pause()
        return

    idempotency_key = f"txn-{uuid.uuid4()}"
    payload = {
        "from_account_id": from_id,
        "to_account_id":   to_id,
        "amount":          float(amount),
        "idempotency_key": idempotency_key,
    }

    success, result = spinner_call(
        "Processing transfer",
        api_post,
        "/api/v1/transactions/send",
        payload,
    )

    console.print()
    if success:
        console.print(Panel(
            f"[bold green]>> {result.get('message', 'Transfer successful')}[/]\n\n"
            f"[dim]Idempotency Key: {idempotency_key}[/]",
            title="[green]>> Success[/]",
            border_style="green",
        ))
    else:
        detail = result.get("detail", result)
        if isinstance(detail, dict):
            msg  = detail.get("message", "Transfer failed")
            code = detail.get("error_code", "")
        else:
            msg, code = str(detail), ""

        console.print(Panel(
            f"[red]!! {msg}[/]\n[dim]Error Code: {code}[/]",
            title="[red]!! Transfer Failed[/]",
            border_style="red",
        ))

    _pause()


def screen_account_info():
    clear()
    console.print(Rule("[bold cyan] Account Info [/]"))
    console.print()

    account_id = Prompt.ask("[cyan]Enter Account ID[/]").strip()
    result = spinner_call("Fetching account", api_get, f"/api/v1/accounts/{account_id}")

    if result is None:
        _pause()
        return

    if "error" in str(result.get("status", "")).lower() or "detail" in result:
        msg = result.get("message", result.get("detail", "Not found"))
        console.print(Panel(
            f"[red]!! {msg}[/]",
            title="[red]Error[/]", border_style="red"
        ))
        _pause()
        return

    table = Table(box=box.SIMPLE_HEAD, border_style="cyan", show_header=False)
    table.add_column("Field", style="dim", width=18)
    table.add_column("Value", style="bold white")

    fields = [
        ("Account ID",   result.get("account_id", account_id)),
        ("Owner ID",     result.get("owner_id", "—")),
        ("Balance",      fmt_money(result.get("balance", 0))),
        ("Currency",     result.get("currency", CURRENCY)),
        ("Status",       result.get("status", "—").upper()),
        ("Created At",   result.get("created_at", "—")),
        ("Updated At",   result.get("updated_at", "—")),
    ]

    for label, val in fields:
        table.add_row(label, str(val))

    console.print(Panel(table, title="[cyan]Account Details[/]", border_style="cyan"))
    _pause()


def screen_health():
    clear()
    console.print(Rule("[bold cyan] System Health [/]"))
    console.print()

    result = spinner_call("Pinging server", api_get, "/health")

    if result is None:
        console.print(Panel(
            "[red]✗ Server is unreachable[/]\n[dim]Make sure the API is running on localhost:8000[/]",
            title="[red]Offline[/]", border_style="red"
        ))
        _pause()
        return

    status  = result.get("status", "unknown")
    message = result.get("message", "")
    is_ok   = status == "healthy"

    color   = "green" if is_ok else "red"
    icon    = "OK" if is_ok else "!!"

    table = Table(box=box.SIMPLE_HEAD, border_style=color, show_header=False)
    table.add_column("", style="dim", width=18)
    table.add_column("", style="bold white")
    table.add_row("Status",  f"[bold {color}]{icon} {status.upper()}[/]")
    table.add_row("Message", message)
    table.add_row("API URL",  BASE_URL)

    console.print(Panel(table, title=f"[{color}]Server Status[/]", border_style=color))
    _pause()


def _pause():
    console.print()
    Prompt.ask("[dim]Press Enter to return to menu[/]", default="", show_default=False)


# ── Main Loop ──────────────────────────────────────────────────────────────────

def main():
    clear()
    show_banner()

    # Quick connectivity check on startup
    with console.status("[cyan]Connecting to server...[/]", spinner="dots"):
        result = api_get("/health")
        time.sleep(0.5)

    if result and result.get("status") == "healthy":
        console.print(Align.center(
            Text("[OK] Server Connected", style="bold green")
        ))
    else:
        console.print(Align.center(
            Text("[!!] Server Offline - some features may not work", style="bold red")
        ))

    console.print()

    handlers = {
        "1": screen_check_balance,
        "2": screen_send_money,
        "3": screen_account_info,
        "4": screen_health,
    }

    while True:
        clear()
        show_banner()
        choice = show_main_menu()

        if choice == "5":
            clear()
            console.print(Align.center(
                Panel("[bold cyan]Thank you for using Mobile Money Wallet![/]",
                      border_style="cyan", padding=(1, 6))
            ))
            console.print()
            sys.exit(0)

        handler = handlers.get(choice)
        if handler:
            clear()
            handler()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted. Goodbye.[/]")
        sys.exit(0)
