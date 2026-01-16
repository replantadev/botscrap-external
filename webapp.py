#!/usr/bin/env python3
"""
BotScrap External - Web Dashboard
Panel de control visual para gestionar los bots
"""

import os
import json
import threading
import subprocess
import time
from datetime import datetime
from pathlib import Path
from functools import wraps

from flask import Flask, render_template, request, jsonify, redirect, url_for, session, Response
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'botscrap-secret-change-me-2024')

# Configuración
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / 'logs'
DATA_DIR = BASE_DIR / 'data'

# Credenciales de acceso (configurables en .env)
ADMIN_USER = os.getenv('ADMIN_USER', 'admin')
ADMIN_PASS = os.getenv('ADMIN_PASS', 'replanta2024')  # ¡CAMBIAR EN PRODUCCIÓN!
ACCESS_PATH = os.getenv('ACCESS_PATH', '/panel')  # URL oculta

# Estado global de los bots
bot_processes = {}
bot_status = {
    'direct': {'running': False, 'pid': None, 'last_run': None, 'last_result': None},
    'resentment': {'running': False, 'pid': None, 'last_run': None, 'last_result': None},
    'social': {'running': False, 'pid': None, 'last_run': None, 'last_result': None},
}

# Lock para thread safety
status_lock = threading.Lock()


def require_auth(f):
    """Decorador para requerir autenticación"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def run_bot_async(bot_type: str, args: list):
    """Ejecutar bot en background"""
    global bot_processes, bot_status
    
    with status_lock:
        if bot_status[bot_type]['running']:
            return False, "Bot ya está ejecutándose"
    
    try:
        # Construir comando
        cmd = ['python', 'run_bot.py', bot_type] + args
        
        # Log file
        log_file = LOGS_DIR / f'{bot_type}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                cwd=str(BASE_DIR),
                text=True
            )
        
        with status_lock:
            bot_processes[bot_type] = process
            bot_status[bot_type]['running'] = True
            bot_status[bot_type]['pid'] = process.pid
            bot_status[bot_type]['last_run'] = datetime.now().isoformat()
            bot_status[bot_type]['log_file'] = str(log_file)
        
        # Thread para monitorear finalización
        def monitor():
            process.wait()
            with status_lock:
                bot_status[bot_type]['running'] = False
                bot_status[bot_type]['pid'] = None
                bot_status[bot_type]['last_result'] = 'completed' if process.returncode == 0 else 'error'
        
        threading.Thread(target=monitor, daemon=True).start()
        
        return True, f"Bot iniciado (PID: {process.pid})"
        
    except Exception as e:
        return False, str(e)


def stop_bot(bot_type: str):
    """Detener un bot"""
    global bot_processes, bot_status
    
    with status_lock:
        if not bot_status[bot_type]['running']:
            return False, "Bot no está ejecutándose"
        
        process = bot_processes.get(bot_type)
        if process:
            process.terminate()
            try:
                process.wait(timeout=5)
            except:
                process.kill()
            
            bot_status[bot_type]['running'] = False
            bot_status[bot_type]['pid'] = None
            bot_status[bot_type]['last_result'] = 'stopped'
            
            return True, "Bot detenido"
    
    return False, "No se pudo detener"


# ============ RUTAS ============

@app.route('/')
def index():
    """Redirigir a login o mostrar 404 fake"""
    # Mostrar página 404 falsa para ocultar el panel
    return "<!DOCTYPE html><html><head><title>404 Not Found</title></head><body><h1>Not Found</h1><p>The requested URL was not found on this server.</p></body></html>", 404


@app.route(ACCESS_PATH)
def panel_redirect():
    """Redirigir al login desde la URL oculta"""
    return redirect(url_for('login'))


@app.route(f'{ACCESS_PATH}/login', methods=['GET', 'POST'])
def login():
    """Página de login"""
    error = None
    
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        
        if username == ADMIN_USER and password == ADMIN_PASS:
            session['authenticated'] = True
            session['username'] = username
            return redirect(url_for('dashboard'))
        else:
            error = 'Credenciales inválidas'
    
    return render_template('login.html', error=error, access_path=ACCESS_PATH)


@app.route(f'{ACCESS_PATH}/logout')
def logout():
    """Cerrar sesión"""
    session.clear()
    return redirect(url_for('login'))


@app.route(f'{ACCESS_PATH}/dashboard')
@require_auth
def dashboard():
    """Dashboard principal"""
    return render_template('dashboard.html', 
                          bot_status=bot_status,
                          access_path=ACCESS_PATH)


@app.route(f'{ACCESS_PATH}/api/status')
@require_auth
def api_status():
    """API: Estado de los bots"""
    with status_lock:
        return jsonify(bot_status)


@app.route(f'{ACCESS_PATH}/api/start', methods=['POST'])
@require_auth
def api_start():
    """API: Iniciar un bot"""
    data = request.json or {}
    bot_type = data.get('bot_type')
    args = data.get('args', [])
    
    if bot_type not in ['direct', 'resentment', 'social']:
        return jsonify({'success': False, 'error': 'Tipo de bot inválido'})
    
    success, message = run_bot_async(bot_type, args)
    return jsonify({'success': success, 'message': message})


@app.route(f'{ACCESS_PATH}/api/stop', methods=['POST'])
@require_auth
def api_stop():
    """API: Detener un bot"""
    data = request.json or {}
    bot_type = data.get('bot_type')
    
    if bot_type not in ['direct', 'resentment', 'social']:
        return jsonify({'success': False, 'error': 'Tipo de bot inválido'})
    
    success, message = stop_bot(bot_type)
    return jsonify({'success': success, 'message': message})


@app.route(f'{ACCESS_PATH}/api/logs/<bot_type>')
@require_auth
def api_logs(bot_type: str):
    """API: Obtener logs de un bot"""
    if bot_type not in ['direct', 'resentment', 'social', 'all']:
        return jsonify({'error': 'Bot inválido'})
    
    # Si pide todos los logs, buscar el más reciente de cada tipo
    if bot_type == 'all':
        all_logs = []
        for bt in ['direct', 'resentment', 'social']:
            log_files = sorted(LOGS_DIR.glob(f'{bt}_*.log'), reverse=True)
            if log_files:
                with open(log_files[0], 'r') as f:
                    all_logs.append(f"=== {bt.upper()} ({log_files[0].name}) ===\n")
                    all_logs.append(f.read()[-5000:])  # Últimos 5KB
                    all_logs.append("\n\n")
        return jsonify({'logs': ''.join(all_logs) if all_logs else 'No hay logs'})
    
    # Log específico del estado actual o el más reciente
    log_file = bot_status.get(bot_type, {}).get('log_file')
    
    if not log_file or not Path(log_file).exists():
        # Buscar el log más reciente de este tipo
        log_files = sorted(LOGS_DIR.glob(f'{bot_type}_*.log'), reverse=True)
        if log_files:
            log_file = str(log_files[0])
    
    if log_file and Path(log_file).exists():
        with open(log_file, 'r') as f:
            content = f.read()
            return jsonify({'logs': content, 'file': Path(log_file).name})
    
    return jsonify({'logs': 'No hay logs disponibles'})


@app.route(f'{ACCESS_PATH}/api/logs-list')
@require_auth
def api_logs_list():
    """API: Listar todos los archivos de log"""
    logs = []
    for log_file in sorted(LOGS_DIR.glob('*.log'), reverse=True)[:50]:
        stat = log_file.stat()
        logs.append({
            'name': log_file.name,
            'size': stat.st_size,
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'bot_type': log_file.stem.split('_')[0] if '_' in log_file.stem else 'unknown'
        })
    return jsonify({'logs': logs})


@app.route(f'{ACCESS_PATH}/api/logs-file/<filename>')
@require_auth
def api_log_file(filename: str):
    """API: Obtener contenido de un archivo de log específico"""
    # Sanitizar nombre de archivo
    safe_name = Path(filename).name
    log_file = LOGS_DIR / safe_name
    
    if not log_file.exists() or not str(log_file).endswith('.log'):
        return jsonify({'error': 'Archivo no encontrado'}), 404
    
    with open(log_file, 'r') as f:
        return jsonify({'logs': f.read(), 'file': safe_name})


@app.route(f'{ACCESS_PATH}/logs')
@require_auth
def logs_page():
    """Página de logs en tiempo real"""
    return render_template('logs.html', access_path=ACCESS_PATH)


@app.route(f'{ACCESS_PATH}/api/logs/<bot_type>/stream')
@require_auth
def api_logs_stream(bot_type: str):
    """API: Stream de logs en tiempo real"""
    if bot_type not in ['direct', 'resentment', 'social']:
        return "Bot inválido", 400
    
    log_file = bot_status.get(bot_type, {}).get('log_file')
    
    def generate():
        if not log_file or not Path(log_file).exists():
            yield "data: No hay logs\n\n"
            return
        
        with open(log_file, 'r') as f:
            # Ir al final
            f.seek(0, 2)
            
            while True:
                line = f.readline()
                if line:
                    yield f"data: {line}\n\n"
                else:
                    time.sleep(0.5)
                    # Verificar si el bot sigue corriendo
                    if not bot_status[bot_type]['running']:
                        yield "data: [Bot finalizado]\n\n"
                        break
    
    return Response(generate(), mimetype='text/event-stream')


@app.route(f'{ACCESS_PATH}/api/test-connection')
@require_auth
def api_test_connection():
    """API: Test de conexión con StaffKit"""
    from staffkit_client import StaffKitClient
    
    client = StaffKitClient()
    result = client.test_connection()
    
    return jsonify(result)


@app.route(f'{ACCESS_PATH}/api/config')
@require_auth
def api_config():
    """API: Ver configuración actual (sin secrets)"""
    from config import (
        STAFFKIT_URL, STAFFKIT_LIST_ID, 
        MAX_LEADS_PER_RUN, DAILY_LIMIT,
        GOOGLE_API_KEY, TELEGRAM_TOKEN
    )
    
    return jsonify({
        'staffkit_url': STAFFKIT_URL,
        'staffkit_list_id': STAFFKIT_LIST_ID,
        'max_leads_per_run': MAX_LEADS_PER_RUN,
        'daily_limit': DAILY_LIMIT,
        'google_api': '✓ Configurado' if GOOGLE_API_KEY else '✗ No configurado',
        'telegram': '✓ Configurado' if TELEGRAM_TOKEN else '✗ No configurado',
    })


# ============ ENDPOINTS DE ACTUALIZACIÓN ============

@app.route(f'{ACCESS_PATH}/api/updates/check')
@require_auth
def api_check_updates():
    """API: Verificar actualizaciones disponibles"""
    from updater import get_updater
    from dataclasses import asdict
    
    updater = get_updater()
    update_info = updater.check_for_updates()
    
    # Convertir commits a dict
    result = {
        'current_version': update_info.current_version,
        'latest_version': update_info.latest_version,
        'commits_behind': update_info.commits_behind,
        'has_updates': update_info.has_updates,
        'last_check': update_info.last_check,
        'commits': [
            {
                'hash': c.hash,
                'short_hash': c.short_hash,
                'author': c.author,
                'date': c.date,
                'message': c.message
            }
            for c in update_info.commits
        ]
    }
    
    return jsonify(result)


@app.route(f'{ACCESS_PATH}/api/updates/pull', methods=['POST'])
@require_auth
def api_pull_updates():
    """API: Aplicar actualizaciones"""
    from updater import get_updater
    
    data = request.json or {}
    force = data.get('force', False)
    
    updater = get_updater()
    
    # Verificar cambios locales
    has_changes, changes = updater.get_local_changes()
    
    if has_changes and not force:
        return jsonify({
            'success': False,
            'error': 'Hay cambios locales sin commitear',
            'local_changes': changes,
            'hint': 'Usa force=true para guardar en stash y actualizar'
        })
    
    # Aplicar actualización
    success, message = updater.pull_updates(force=force)
    
    return jsonify({
        'success': success,
        'message': message,
        'new_version': updater.get_current_version() if success else None,
        'restart_required': success  # Indicar que se necesita reiniciar
    })


@app.route(f'{ACCESS_PATH}/api/updates/status')
@require_auth
def api_update_status():
    """API: Estado del repositorio"""
    from updater import get_updater
    
    updater = get_updater()
    status = updater.get_status()
    
    return jsonify(status)


@app.route(f'{ACCESS_PATH}/api/updates/changelog')
@require_auth
def api_changelog():
    """API: Historial de cambios"""
    from updater import get_updater
    
    updater = get_updater()
    limit = request.args.get('limit', 20, type=int)
    
    commits = updater.get_changelog(limit=limit)
    
    return jsonify({
        'commits': [
            {
                'hash': c.hash,
                'short_hash': c.short_hash,
                'author': c.author,
                'date': c.date,
                'message': c.message
            }
            for c in commits
        ]
    })


@app.route(f'{ACCESS_PATH}/api/restart', methods=['POST'])
@require_auth
def api_restart():
    """API: Reiniciar el servidor (después de actualizar)"""
    import sys
    import signal
    
    def delayed_restart():
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGTERM)
    
    threading.Thread(target=delayed_restart, daemon=True).start()
    
    return jsonify({
        'success': True,
        'message': 'Reiniciando servidor...'
    })


if __name__ == '__main__':
    # Crear directorios
    LOGS_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    
    # Modo desarrollo
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    port = int(os.getenv('FLASK_PORT', '5000'))
    
    print(f"\n🚀 BotScrap External - Dashboard")
    print(f"   URL: http://localhost:{port}{ACCESS_PATH}")
    print(f"   User: {ADMIN_USER}")
    print(f"\n")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
