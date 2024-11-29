import os
from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging
import smtplib
import requests

# Configurações
TELEGRAM_BOT_TOKEN = "SEU_TELEGRAM_BOT_TOKEN"
EMAIL_SENDER = "seuemail@example.com"
EMAIL_PASSWORD = "senha"
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587

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

c.subscribe(['image'])

# Formato esperado da mensagem Kafka:
# {"timestamp": 1649288146.3453217, "new_file": "9PKAyoN_rotate.jpeg", "email": "destino@example.com", "telegram_id": "123456789", "transformacao": "rotate"}

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
            transformacao = data.get('transformacao', 'desconhecida')

            # Criar mensagem de notificação baseada na transformação
            if transformacao == "rotate":
                mensagem = f"O arquivo {filename} foi rotacionado."
            elif transformacao == "grayscale":
                mensagem = f"O arquivo {filename} foi convertido para escala de cinza."
            else:
                mensagem = f"Uma transformação desconhecida foi realizada no arquivo {filename}."

            logging.info(f"Notificação criada: {mensagem}")

            # Enviar notificações diretamente (e-mail e Telegram)
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
