from PIL import Image
import os
from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging
import smtplib
import requests

# Configurações
OUT_FOLDER = '/processed/notificador/'
NEW = '_notificador'
IN_FOLDER = "/appdata/static/uploads/"
TELEGRAM_BOT_TOKEN = "SEU_TELEGRAM_BOT_TOKEN"
EMAIL_SENDER = "seuemail@example.com"
EMAIL_PASSWORD = "senha"
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587

# Função para criar a notificação (rotate como exemplo)
def create_notificador(path_file):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    return filename  # Retorna o nome do arquivo original

# Função para enviar mensagens no Telegram
def enviar_telegram(telegram_id, mensagem):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {"chat_id": telegram_id, "text": mensagem}
    try:
        requests.post(url, data=data)
        logging.info(f"Mensagem enviada no Telegram para {telegram_id}")
    except Exception as e:
        logging.error(f"Erro ao enviar mensagem no Telegram: {e}")

# Função para enviar e-mail
def enviar_email(email, mensagem):
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, email, mensagem)
            logging.info(f"E-mail enviado para {email}")
    except Exception as e:
        logging.error(f"Erro ao enviar e-mail: {e}")

# Configuração do consumidor Kafka
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificador-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

# Configuração do produtor Kafka
p = Producer({'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093'})

c.subscribe(['image'])
# Formato esperado: {"timestamp": 1649288146.3453217, "new_file": "9PKAyoN.jpeg", "email": "destino@example.com", "telegram_id": "123456789"}

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            email = data.get('email', None)
            telegram_id = data.get('telegram_id', None)
            
            logging.warning(f"PROCESSANDO {filename}")
            arquivo_original = create_notificador(IN_FOLDER + filename)
            logging.warning(f"FINALIZADO {filename}")

            # Criar mensagem de notificação
            mensagem = f"O arquivo {arquivo_original} foi rotacionado."
            
            # Publicar no tópico de notificação
            notificacao = {
                "arquivo_original": arquivo_original,
                "mensagem": mensagem,
                "email": email,
                "telegram_id": telegram_id
            }
            p.produce('notificacao', json.dumps(notificacao))
            p.flush()

            # Enviar notificações diretamente (opcional)
            if email:
                enviar_email(email, mensagem)
            if telegram_id:
                enviar_telegram(telegram_id, mensagem)

        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('Fim da partição alcançado {0}/{1}'
                            .format(msg.topic(), msg.partition()))
        else:
            logging.error('Erro ocorrido: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()
    p.flush()
