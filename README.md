Api creada en python con ayuda de Geminis que permite realizar las siguientes acciones.

1. Valida estructura de archivos zip y pdf en su contenido
2. Almacena cada archivo zip en mongoDB
3. Procesa cada archivo zip, descomprimiendo cada archivo y reduciondo el tamaño de los archivos PDF
4. Guarda en un SFTP los archivos PDF y notifica el resultado del procesamiento de cada archivo zip

archivos usados (validador.py - worker.py)

# Ing Nivek.
