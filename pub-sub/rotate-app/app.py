from PIL import Image, ImageOps
import os
from confluent_kafka import Consumer, Producer, KafkaError
import json
import logging

OUT_FOLDER = '/processed/rotate/'
NEW = '_rotate'
IN_FOLDER = "/appdata/static/uploads/"

# Configuração do produtor Kafka
p = Producer({'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093'})

# Função para rotacionar a imagem
def create_rotate(path_file):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    original_image = Image.open(path_file)
    transposed = original_image.transpose(Image.Transpose.ROTATE_180)

    name, ext = os.path.splitext(filename)
    transposed.save(output_folder + name + NEW + ext)

    return filename  # Retorna o nome do arquivo original


# Função para publicar a transformação no Kafka
def publicar_transformacao(filename):
    notificacao = {
        "timestamp": 1649288146.3453217,
        "new_file": filename,
        "transformacao": "rotate"  # Campo indicando que a transformação foi uma rotação
    }
    p.produce('image', json.dumps(notificacao))
    p.flush()


### Configuração do consumidor Kafka
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'rotate-group',
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
            # Rotaciona a imagem
            arquivo_original = create_rotate(IN_FOLDER + filename)
            logging.warning(f"FINALIZADO {arquivo_original}")

            # Publica no Kafka que a imagem foi rotacionada
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
