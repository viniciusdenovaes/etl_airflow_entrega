## Segunda entrega desafio Raizen

Feito usando airflow

Para rodar é preciso ter Docker e Docker-compose instalado

1. Comando para construir a imagem:
```
sudo docker compose build
```
Este comando deve demorar por volta de 5 minutos,
sendo 2 minutos para instalar o python e requirements e
quase 3 minutos para instalar o libreoffice.

O libreoffice foi a única forma que eu encontrei de ler este xls.
Outras tentativas foram: usar o pandas, usar um pacote do python,
usar o operador de XLSX do airflow


2. Comando para inicializar o database do airflow, com usuario e senha `airflow`
```
sudo docker compose up airflow-init
```

3. comando para executar:
```
sudo docker compose up
```

Após este comando acessar [http://localhost:8080/] e executar o dag `etl_all_pandas_vinicius`, com as tags "raizen" e "entrega".

4. Para parar o airflow e liberar a porta 8080
```
sudo docker compose down --volumes --rmi all
```
