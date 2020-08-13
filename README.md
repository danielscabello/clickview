
INTRODUÇÃO

Esse teste foi executado no Ubuntu, utilizando SPARK e pyspark, para fazer o setup eu utilizei o guia abaixo:

https://datawookie.netlify.app/blog/2017/07/installing-spark-on-ubuntu/

Uma vez o pyspark estiver setado, os dados devem ser carregados no diretório data, importante resaltar que o script vai carregar todos os arquivos do diretório (pode manter com gzip)

A intenção do SPARK é para manipulação de grande volume de dados, e com Pyspark podemos combinar a linguagem python para facilitar a implementação

Items entregues:
    • Etapa1 totalmente entregue com sessionamento e formatação
    • Etapa2 entregue com problemas de formatação do json 

Não consegui fazer por questão de tempo.
    • estrutura do json file para Etapa 2
    • Etapa3 inteira.


INSTRUCOES

1 - Copiar o script para o diretório corrente

2 - Copiar os dados pro diretório "data" do diretorio corrente

3 - executar :
$SPARK_HOME/bin/spark-submit start.py

4 - Será criado 2 diretórios com os arquivos json dentro deles.

etapa1
etapa2

Example:
daniel@home1:~/escale-test$ find etapa1 etapa2
etapa1
etapa1/.part-00000-e363caae-5ed0-4700-bf95-65268df54621-c000.txt.crc
etapa1/._SUCCESS.crc
etapa1/_SUCCESS
etapa1/part-00000-e363caae-5ed0-4700-bf95-65268df54621-c000.txt
etapa2
etapa2/.part-00000-f27ab108-d4f6-43b4-9b69-33e291927bad-c000.json.crc
etapa2/part-00000-f27ab108-d4f6-43b4-9b69-33e291927bad-c000.json
etapa2/._SUCCESS.crc
etapa2/_SUCCESS
