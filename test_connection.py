#!/usr/bin/env python3
"""
Test de conexión con StaffKit
Ejecutar antes de usar el bot para verificar configuración
"""

import sys
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from config import validate_config, STAFFKIT_URL, STAFFKIT_API_KEY, STAFFKIT_LIST_ID
from staffkit_client import StaffKitClient

console = Console()


def test_connection():
    """Probar conexión con StaffKit"""
    
    console.print("\n[bold blue]🔌 Test de Conexión - BotScrap External[/bold blue]\n")
    
    # 1. Validar configuración
    console.print("[yellow]1. Validando configuración...[/yellow]")
    validation = validate_config()
    
    if validation['errors']:
        console.print("[red]❌ Errores de configuración:[/red]")
        for error in validation['errors']:
            console.print(f"   • {error}")
        return False
    
    if validation['warnings']:
        console.print("[yellow]⚠️ Advertencias:[/yellow]")
        for warning in validation['warnings']:
            console.print(f"   • {warning}")
    
    console.print("[green]✅ Configuración válida[/green]\n")
    
    # 2. Probar conexión
    console.print("[yellow]2. Probando conexión con StaffKit...[/yellow]")
    console.print(f"   URL: {STAFFKIT_URL}")
    console.print(f"   API Key: {STAFFKIT_API_KEY[:10]}...{STAFFKIT_API_KEY[-4:]}")
    
    client = StaffKitClient()
    result = client.test_connection()
    
    if result['success']:
        console.print(f"[green]✅ Conexión exitosa (HTTP {result.get('status_code', 200)})[/green]\n")
    else:
        console.print(f"[red]❌ Error de conexión: {result.get('error', 'Unknown')}[/red]")
        return False
    
    # 3. Probar verificación de duplicados
    console.print("[yellow]3. Probando endpoint de duplicados...[/yellow]")
    try:
        is_dup = client.check_duplicate("example-test-domain.com")
        console.print(f"[green]✅ Check duplicate funciona (result: {is_dup})[/green]\n")
    except Exception as e:
        console.print(f"[yellow]⚠️ Check duplicate no disponible: {e}[/yellow]\n")
    
    # 4. Mostrar resumen
    table = Table(title="📊 Resumen de Configuración")
    table.add_column("Parámetro", style="cyan")
    table.add_column("Valor", style="green")
    table.add_column("Estado", style="yellow")
    
    table.add_row("StaffKit URL", STAFFKIT_URL, "✅")
    table.add_row("API Key", f"{STAFFKIT_API_KEY[:10]}...", "✅")
    table.add_row("List ID", str(STAFFKIT_LIST_ID), "✅")
    
    from config import GOOGLE_API_KEY, CX_ID, TELEGRAM_TOKEN
    
    table.add_row(
        "Google API", 
        f"{GOOGLE_API_KEY[:10]}..." if GOOGLE_API_KEY else "No configurado",
        "✅" if GOOGLE_API_KEY else "⚠️"
    )
    table.add_row(
        "CX ID", 
        CX_ID if CX_ID else "No configurado",
        "✅" if CX_ID else "⚠️"
    )
    table.add_row(
        "Telegram", 
        f"{TELEGRAM_TOKEN[:10]}..." if TELEGRAM_TOKEN else "No configurado",
        "✅" if TELEGRAM_TOKEN else "⚠️"
    )
    
    console.print(table)
    
    console.print(Panel(
        "[green bold]✅ Todo listo para ejecutar el bot![/green bold]\n\n"
        "Prueba con:\n"
        "  [cyan]python run_bot.py direct --query 'wordpress madrid' --limit 5[/cyan]\n"
        "  [cyan]python run_bot.py resentment --hosting hostinger --limit 10[/cyan]",
        title="🚀 Ready"
    ))
    
    return True


if __name__ == '__main__':
    success = test_connection()
    sys.exit(0 if success else 1)
