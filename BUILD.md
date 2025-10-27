```bash
crm_automation/
│
├── backend/
│ ├── **init**.py
│ ├── main.py # FastAPI backend (API para controlar los procesos)
│ ├── db/
│ │ ├── **init**.py
│ │ ├── models.py # Definición de tablas (SQLite para logs/config)
│ │ └── database.py # Conexión y creación de la base local
│ │
│ ├── crms/ # Scripts específicos por CRM
│ │ ├── **init**.py
│ │ ├── base_scraper.py # Clase base para manejar Selenium
│ │ ├── crm_a_scraper.py # Automatiza descargas de CRM A
│ │ ├── crm_b_scraper.py # Automatiza descargas de CRM B
│ │ └── crm_c_scraper.py # …
│ │
│ ├── processing/
│ │ ├── **init**.py
│ │ ├── cleaner.py # Limpieza y unión de CSV con pandas
│ │ ├── transformer.py # Transformaciones adicionales (queries locales)
│ │ └── uploader.py # Subida a BigQuery
│ │
│ ├── scheduler/
│ │ ├── **init**.py
│ │ ├── jobs.py # Funciones programadas con APScheduler
│ │ └── scheduler.py # Configuración y control de tareas
│ │
│ ├── services/
│ │ ├── **init**.py
│ │ ├── file_manager.py # Mover CSV, limpieza de carpetas temporales
│ │ ├── logger.py # Log centralizado (SQLite + archivo .log)
│ │ └── notifier.py # Envío de notificaciones (email/telegram)
│ │
│ └── config.py # Variables globales (rutas, credenciales, etc.)
│
├── frontend/
│ ├── **init**.py
│ ├── main.py # PySide6 App principal
│ ├── ui/
│ │ ├── main_window.ui # Diseño base (Qt Designer)
│ │ ├── resources.qrc # Íconos, imágenes
│ │ └── compiled_resources_rc.py
│ │
│ ├── components/
│ │ ├── crm_card.py # Componente visual para cada CRM
│ │ ├── log_viewer.py # Visualizador de logs
│ │ └── settings_dialog.py # Configuración manual (intervalos, credenciales)
│ │
│ ├── controllers/
│ │ ├── dashboard_controller.py # Control de eventos principales
│ │ └── settings_controller.py # Lógica del panel de configuración
│ │
│ ├── assets/
│ │ ├── icons/
│ │ └── css/
│ │
│ └── styles/
│ ├── theme.qss # Estilos globales (modo oscuro/claro)
│ └── widgets.qss
│
├── scripts/
│ ├── run_all.py # Ejecuta todos los CRMs sin GUI (para cron jobs)
│ ├── test_crm.py # Prueba un CRM en particular
│ └── init_db.py # Inicializa base SQLite y tablas
│
├── .env # Variables de entorno (credenciales)
├── requirements.txt # Dependencias del proyecto
├── README.md # Documentación
└── start_app.py # Punto de entrada principal (inicia GUI y backend)
```
