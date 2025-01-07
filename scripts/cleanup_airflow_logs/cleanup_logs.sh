#!/bin/bash

# Define o diretório onde os logs estão localizados
LOG_DIR="/home/ubuntu/plataforma-dados-alloha-airflow/infra/logs"

# Obtém a data atual no formato desejado (por exemplo, dd/mm/yyyy)
CURRENT_DATE=$(date +"%d/%m/%Y")

# Mostra o tamanho do diretório antes da limpeza
SIZE_BEFORE=$(du -sh "$LOG_DIR" | cut -f1)
echo "Tamanho da pasta $LOG_DIR antes da limpeza: $SIZE_BEFORE"

# Remove arquivos com mais de 7 dias
find "$LOG_DIR" -type f -mtime +7 -exec rm -f {} \;

# Verifica se o comando anterior foi executado com sucesso
if [ $? -eq 0 ]; then
    echo "Na data $CURRENT_DATE o script executou com sucesso e os logs antigos foram removidos."

    # Mostra o tamanho do diretório após a limpeza
    SIZE_AFTER=$(du -sh "$LOG_DIR" | cut -f1)
    echo "Tamanho da pasta $LOG_DIR após a limpeza: $SIZE_AFTER"
else
    echo "O script não executou com sucesso na data $CURRENT_DATE."
fi
