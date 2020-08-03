
INTRODUÇÃO

Esse teste foi executado no Ubuntu, utilizando SPARK e pyspark, para fazer o setup eu utilizei o guia abaixo:

https://datawookie.netlify.app/blog/2017/07/installing-spark-on-ubuntu/

Uma vez o pyspark estiver setado, os dados devem ser carregados no diretório data, importante resaltar que o script vai carregar todos os arquivos do diretório (pode manter com gzip)

A intenção do SPARK é para manipulação de grande volume de dados. Nunca havia mexido com Spark antes então tive que aprender um pouco antes de seguir pensando a solução.

Não consegui fazer por questão de tempo.
    • Sessionamento
    • formatação 100% como descrita no json.
    • json file para Etapa 2
    • Etapa 3


INSTRUCOES

1 - Copiar o script para o diretório e executar:

$SPARK_HOME/bin/spark-submit etapas_v0.py

2 - Será criado 2 diretórios com os arquivos json dentro deles.


]
