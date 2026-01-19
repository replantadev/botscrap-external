#!/usr/bin/env python3
"""
BotScrap External - CLI Principal
Ejecuta los diferentes tipos de bots
"""

import sys
import logging
import click
from rich.console import Console
from rich.logging import RichHandler

from config import validate_config, BOT_NAME
from staffkit_client import StaffKitClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)
console = Console()


@click.group()
@click.version_option(version='1.0.0')
def cli():
    """🤖 BotScrap External - Lead Generation Bot"""
    pass


@cli.command()
@click.option('--query', '-q', required=True, help='Query de búsqueda (ej: "agencia marketing wordpress madrid")')
@click.option('--limit', '-l', default=10, help='Número máximo de leads')
@click.option('--list-id', type=int, help='ID de lista en StaffKit (override .env)')
@click.option('--cms', type=click.Choice(['all', 'wordpress', 'joomla']), default=None, help='Filtrar por CMS')
@click.option('--max-speed', type=int, default=None, help='Max speed score (captar webs lentas)')
@click.option('--eco-only', is_flag=True, help='Solo perfiles ecológicos')
@click.option('--dry-run', is_flag=True, help='No guardar, solo mostrar resultados')
def direct(query: str, limit: int, list_id: int, cms: str, max_speed: int, eco_only: bool, dry_run: bool):
    """🎯 Bot de búsqueda directa en Google"""
    
    console.print(f"\n[bold blue]🎯 Direct Bot - BotScrap External[/bold blue]")
    console.print(f"Query: [cyan]{query}[/cyan]")
    console.print(f"Límite: [cyan]{limit}[/cyan] leads")
    
    # Mostrar filtros activos
    if cms:
        console.print(f"Filtro CMS: [cyan]{cms}[/cyan]")
    if max_speed:
        console.print(f"Max Speed Score: [cyan]{max_speed}[/cyan]")
    if eco_only:
        console.print(f"[green]🌿 Solo perfiles ecológicos[/green]")
    
    console.print()
    
    # Validar config
    validation = validate_config()
    if not validation['valid']:
        console.print("[red]❌ Configuración inválida:[/red]")
        for error in validation['errors']:
            console.print(f"   • {error}")
        sys.exit(1)
    
    from bots.direct_bot import DirectBot
    
    # Construir config de filtros
    config = {}
    if cms:
        config['cms_filter'] = cms
    if max_speed:
        config['max_speed_score'] = max_speed
    if eco_only:
        config['eco_verde_only'] = True
    
    bot = DirectBot(dry_run=dry_run, config=config if config else None)
    results = bot.run(query=query, max_leads=limit, list_id=list_id)
    
    console.print(f"\n[green]✅ Completado:[/green]")
    console.print(f"   Encontrados: {results.get('leads_found', 0)}")
    console.print(f"   Guardados: {results.get('leads_saved', 0)}")
    console.print(f"   Duplicados: {results.get('leads_duplicates', 0)}")


@cli.command()
@click.option('--hosting', '-h', help='Hosting competidor (ej: hostinger, godaddy)')
@click.option('--all-hostings', is_flag=True, help='Buscar en todos los hostings conocidos')
@click.option('--limit', '-l', default=20, help='Número máximo de leads')
@click.option('--list-id', type=int, help='ID de lista en StaffKit')
@click.option('--dry-run', is_flag=True, help='No guardar, solo mostrar resultados')
def resentment(hosting: str, all_hostings: bool, limit: int, list_id: int, dry_run: bool):
    """😤 Resentment Hunter - Busca clientes frustrados en reviews"""
    
    console.print(f"\n[bold red]😤 Resentment Hunter - BotScrap External[/bold red]")
    
    if not hosting and not all_hostings:
        console.print("[red]Especifica --hosting o --all-hostings[/red]")
        sys.exit(1)
    
    validation = validate_config()
    if not validation['valid']:
        console.print("[red]❌ Configuración inválida[/red]")
        sys.exit(1)
    
    from bots.resentment_bot import ResentmentBot
    
    bot = ResentmentBot(dry_run=dry_run)
    
    if all_hostings:
        console.print(f"Buscando en [cyan]TODOS[/cyan] los hostings conocidos")
        results = bot.run_all(max_leads=limit, list_id=list_id)
    else:
        console.print(f"Buscando reviews de [cyan]{hosting}[/cyan]")
        results = bot.run(hosting=hosting, max_leads=limit, list_id=list_id)
    
    console.print(f"\n[green]✅ Completado:[/green]")
    console.print(f"   Encontrados: {results.get('leads_found', 0)}")
    console.print(f"   Guardados: {results.get('leads_saved', 0)}")
    console.print(f"   Duplicados: {results.get('leads_duplicates', 0)}")


@cli.command()
@click.option('--sources', '-s', default='reddit', help='Fuentes: reddit,twitter (separadas por coma)')
@click.option('--limit', '-l', default=15, help='Número máximo de leads')
@click.option('--list-id', type=int, help='ID de lista en StaffKit')
@click.option('--keywords', '-k', help='Keywords personalizadas (separadas por comas)')
@click.option('--dry-run', is_flag=True, help='No guardar, solo mostrar resultados')
def social(sources: str, limit: int, list_id: int, keywords: str, dry_run: bool):
    """📡 Social Signals - Monitorea redes sociales"""
    
    console.print(f"\n[bold cyan]📡 Social Signals Bot - BotScrap External[/bold cyan]")
    console.print(f"Fuentes: [cyan]{sources}[/cyan]")
    if keywords:
        console.print(f"Keywords: [cyan]{keywords}[/cyan]")
    
    validation = validate_config()
    if not validation['valid']:
        console.print("[red]❌ Configuración inválida[/red]")
        sys.exit(1)
    
    from bots.social_bot import SocialBot
    
    source_list = [s.strip() for s in sources.split(',')]
    
    bot = SocialBot(dry_run=dry_run)
    results = bot.run(sources=source_list, max_leads=limit, list_id=list_id, keywords=keywords)
    
    console.print(f"\n[green]✅ Completado:[/green]")
    console.print(f"   Encontrados: {results.get('leads_found', 0)}")
    console.print(f"   Guardados: {results.get('leads_saved', 0)}")


@cli.command()
def test():
    """🔌 Test de conexión con StaffKit"""
    from test_connection import test_connection
    success = test_connection()
    sys.exit(0 if success else 1)


@cli.command()
def hostings():
    """📋 Lista los hostings disponibles para Resentment Hunter"""
    from config import COMPETITOR_HOSTINGS
    
    console.print("\n[bold]Hostings disponibles para Resentment Hunter:[/bold]\n")
    
    for key, data in COMPETITOR_HOSTINGS.items():
        console.print(f"  [cyan]{key:15}[/cyan] → {data['name']}")
    
    console.print(f"\n[dim]Total: {len(COMPETITOR_HOSTINGS)} hostings[/dim]")
    console.print("\nUso: [cyan]python run_bot.py resentment --hosting hostinger[/cyan]")


@cli.command()
@click.option('--test', '-t', is_flag=True, help='Solo probar configuración')
def worker(test: bool):
    """🤖 Worker Autónomo 24/7 - Ejecuta bots programados"""
    
    console.print(f"\n[bold green]🤖 Worker Autónomo - BotScrap External[/bold green]")
    
    # Validar config
    validation = validate_config()
    if not validation['valid']:
        console.print("[red]❌ Configuración inválida:[/red]")
        for error in validation['errors']:
            console.print(f"   • {error}")
        sys.exit(1)
    
    from orchestrator import get_orchestrator
    
    orchestrator = get_orchestrator()
    
    if test:
        console.print("\n[cyan]Probando configuración...[/cyan]")
        orchestrator.setup()
        console.print("[green]✓ Configuración OK[/green]")
        
        # Test Telegram
        if orchestrator.notifier and orchestrator.notifier.enabled:
            console.print("\n[cyan]Probando Telegram...[/cyan]")
            if orchestrator.notifier.test_connection():
                console.print("[green]✓ Telegram OK[/green]")
            else:
                console.print("[yellow]⚠ Telegram no funciona[/yellow]")
        else:
            console.print("[yellow]⚠ Telegram no configurado[/yellow]")
        
        return
    
    console.print("\n[bold]Iniciando worker...[/bold]")
    console.print("Presiona Ctrl+C para detener\n")
    
    try:
        orchestrator.run_forever()
    except KeyboardInterrupt:
        console.print("\n[yellow]Deteniendo worker...[/yellow]")


@cli.command()
def status():
    """📊 Estado del Worker y cola de trabajos"""
    
    console.print(f"\n[bold cyan]📊 Estado del Sistema[/bold cyan]\n")
    
    from orchestrator import get_orchestrator
    
    orchestrator = get_orchestrator()
    orchestrator.setup()
    
    status = orchestrator.get_status()
    
    # Estado del worker
    worker_status = status.get('worker', {})
    running = worker_status.get('running', False)
    paused = worker_status.get('paused', False)
    
    if running and not paused:
        console.print(f"Worker: [green]● Ejecutando[/green]")
    elif paused:
        console.print(f"Worker: [yellow]⏸ Pausado[/yellow]")
    else:
        console.print(f"Worker: [red]○ Detenido[/red]")
    
    # Cola
    queue = status.get('queue', {})
    console.print(f"Cola: [cyan]{queue.get('pending', 0)}[/cyan] pendientes, [yellow]{queue.get('running', 0)}[/yellow] ejecutando")
    
    # Stats
    stats = status.get('stats', {})
    console.print(f"\n[bold]Estadísticas del día:[/bold]")
    console.print(f"  Leads guardados: [green]{stats.get('leads_today', 0)}[/green]")
    console.print(f"  Ejecuciones: {stats.get('runs_today', 0)}")
    console.print(f"  Dominios vistos: {stats.get('total_domains', 0)}")
    
    # Health
    health = status.get('health', {})
    if health:
        healthy = health.get('healthy', False)
        if healthy:
            console.print(f"\nSalud: [green]✓ Todo OK[/green]")
        else:
            console.print(f"\nSalud: [red]✗ Problemas detectados[/red]")
            for check in health.get('checks', []):
                if not check.get('healthy'):
                    console.print(f"  - {check.get('name')}: {check.get('message')}")


@cli.command()
@click.argument('bot_type', type=click.Choice(['direct', 'resentment', 'social']))
@click.option('--priority', '-p', type=int, default=3, help='Prioridad (1=hot, 4=low)')
def queue(bot_type: str, priority: int):
    """📥 Añadir trabajo a la cola"""
    
    console.print(f"\n[cyan]Añadiendo trabajo a la cola...[/cyan]")
    
    from orchestrator import get_orchestrator
    
    orchestrator = get_orchestrator()
    orchestrator.setup()
    
    job_id = orchestrator.add_job(bot_type, priority=priority)
    
    console.print(f"[green]✓ Job añadido: {job_id}[/green]")
    console.print(f"  Tipo: {bot_type}")
    console.print(f"  Prioridad: {priority}")


if __name__ == '__main__':
    cli()
