from PIL import Image, ImageOps
from confluent_kafka import Consumer, Producer, KafkaError
import json
import os
import logging

OUT_FOLDER = '/processed/grayscale/'
NEW = '_grayscale'
IN_FOLDER = "/appdata/static/uploads/"

# Configuração do produtor Kafka
p = Producer({'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093'})

# Função para criar a imagem em escala de cinza
def create_grayscale(path_file):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    original_image = Image.open(path_file)
    gray_image = ImageOps.grayscale(original_image)

    name, ext = os.path.splitext(filename)
    gray_image.save(output_folder + name + NEW + ext)

    return filename  # Retorna o nome do arquivo original


# Função para publicar a transformação no Kafka
def publicar_transformacao(filename):
    notificacao = {
        "timestamp": 1649288146.3453217,
        "new_file": filename,
        "transformacao": "grayscale"  # Campo indicando que a transformação foi grayscale
    }
    p.produce('image', json.dumps(notificacao))
    p.flush()


# Configuração do consumidor Kafka
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'grayscale-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['image'])

# Exemplo de mensagem Kafka esperada: {"timestamp": 1649288146.3453217, "new_file": "9PKAyoN.jpeg"}

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']

            logging.warning(f"PROCESSANDO {filename}")
            # Converte a imagem para escala de cinza
            arquivo_original = create_grayscale(IN_FOLDER + filename)
            logging.warning(f"FINALIZADO {arquivo_original}")

            # Publica no Kafka que a imagem foi convertida para grayscale
            publicar_transformacao(arquivo_original)

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

