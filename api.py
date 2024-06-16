# Criação de uma API REST pública em Python de consulta aos dados do site da Embrapa (http://vitibrasil.cnpuv.embrapa.br/)

# 1) Escolha do framework: usar um framework como Flask ou Django para criar a API. Ambos são populares e amplamente usados para desenvolvimento web em Python. O Flask é mais leve e adequado para APIs simples.
# 2) Definição das rotas da API: definir as rotas da API, que são os URLs aos quais os clientes farão requisições para obter os dados. Normalmente, as rotas são definidas com os métodos HTTP correspondentes (GET, POST, etc.).
# 3) Integração com o site para obter os dados: usar bibliotecas como requests para fazer requisições HTTP ao site e obter os dados desejados. Dependendo do site e de sua estrutura, pode ser necessário analisar o HTML ou JSON retornado para extrair os dados corretamente.
# 4) Formatação dos dados de resposta: Depois de obter os dados do site, deve-se formatá-los de acordo com o formato de resposta desejado (por exemplo, JSON) antes de enviá-los de volta ao cliente.
# 5) Testar no Postman o link http://localhost:5000/dados, clicar em Get Data e Send ou testar na web o link http://localhost:5000/dados


# Documentação

# 1) Documentação Inline (comentários no código)
# 2) Ferramentas de Documentação Automática: Flasgger (OpenAPI) ou Swagger UI
# 3) Acessar a documentação em http://localhost:5000/apidocs

""" 
Cenário e Arquitetura Proposta
1. Coleta de Dados:
    Fonte de Dados: Dados históricos da vitivinicultura, que podem ser obtidos da API criada abaixo do site da Embrapa.
2. Pré-processamento dos Dados:
    Limpeza de Dados: Remoção de dados faltantes, tratamento de outliers, normalização dos dados, etc.
    Engenharia de Recursos: Criação de novos atributos relevantes para o modelo, como características dos imóveis, localização, área, etc.
3. Treinamento do Modelo:
    Seleção do Modelo: Escolha de um modelo de regressão adequado, como regressão linear, árvores de decisão, ou modelos mais avançados como XGBoost ou redes neurais.
    Treinamento do Modelo: Utilização dos dados históricos para treinar o modelo escolhido.
4. Validação do Modelo:
    Validação Cruzada: Avaliação do desempenho do modelo utilizando técnicas como validação cruzada para garantir sua robustez.
5. Desenvolvimento da API:
    Framework: Implementação da API REST utilizando Flask em Python, como exemplificado anteriormente.
    Documentação da API: Utilização de ferramentas como Flasgger para documentar os endpoints da API.
6. Deploy da API:
    Ambiente de Produção: Escolha de uma plataforma de nuvem, como AWS, Google Cloud Platform ou Azure.
    Configuração do Servidor: Configuração de um servidor web (por exemplo, usando EC2 na AWS) para hospedar a aplicação Flask.
    Conteinerização: Opcionalmente, pode-se utilizar Docker para facilitar o empacotamento da aplicação e suas dependências.
7. Ingestão de Dados em Tempo Real:
    Pipeline de Ingestão: Implementação de um pipeline para ingestão de novos dados da vitivinicultura em tempo real, utilizando serviços como Kafka, AWS Kinesis ou Google Cloud Pub/Sub.
    Atualização do Modelo: Implementação de um mecanismo para atualização periódica do modelo com novos dados.
8. Monitoramento e Escalabilidade:
    Monitoramento: Implementação de monitoramento contínuo da API e do modelo para garantir desempenho e disponibilidade.
    Escalabilidade: Configuração automática de escalabilidade para lidar com aumentos na carga de requisições, utilizando serviços de balanceamento de carga e autoescalonamento.
9. Segurança:
    Autenticação e Autorização: Implementação de autenticação e autorização adequadas para proteger a API contra acessos não autorizados.
    Segurança dos Dados: Garantia da segurança dos dados de entrada e saída da API.
"""

# MVP do deploy: link do Github https://github.com/lcblcblcb10/API_Embrapa

from flask import Flask, jsonify
from flasgger import Swagger
import requests
from bs4 import BeautifulSoup
import pandas as pd

app = Flask(__name__)
swagger = Swagger(app)

# Função para consultar os dados para um ano específico
def consultar_dados_por_ano(ano):
    try:
        dados_concatenados = []

        # Itera sobre os valores de x de 2 a 6
        for x in range(2, 7):
            # Itera sobre os valores de y dependendo do valor de x
            if x == 2 or x == 4:
                y_range = range(1, 5)
            elif x == 3:
                y_range = range(1, 5)
            elif x == 5:
                y_range = range(1, 6)
            elif x == 6:
                y_range = range(1, 5)

            # Para cada combinação de x e y, consulta os dados
            for y in y_range:
                # Monta a URL com base nas condições de x e y
                if x == 2 or x == 4:
                    url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{x}"
                elif x == 3 or x == 5 or x == 6:
                    url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{x}&subopcao=subopt_0{y}"

                # Consulta ao site
                response = requests.get(url)

                # Se a solicitação foi bem-sucedida (código de status 200)
                if response.status_code == 200:
                    # Analisa o HTML e encontra a tabela desejada (o 4º dataframe)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    tables = pd.read_html(str(soup))
                    df = tables[3]  # 4º DataFrame

                    # Criar a coluna "Aba" com base no valor de x
                    if x == 2:
                        df['Aba'] = 'Produção'
                    elif x == 3:
                        df['Aba'] = 'Processamento'
                    elif x == 4:
                        df['Aba'] = 'Comercialização'
                    elif x == 5:
                        df['Aba'] = 'Importação'
                    elif x == 6:
                        df['Aba'] = 'Exportação'

                    # Criar a coluna "Categoria" com base no valor de y
                    if x == 2 or x == 4:
                        df['Categoria'] = None
                    elif x == 3:
                        df['Categoria'] = categorizar_processamento(y)
                    elif x == 5:
                        df['Categoria'] = categorizar_importacao(y)
                    elif x == 6:
                        df['Categoria'] = categorizar_exportacao(y)

                    # Adiciona a coluna de ano
                    df['Ano'] = ano

                    # Remove linhas onde 'Produto' é NaN
                    #df = df.dropna(subset=['Produto'])

                    dados_concatenados.extend(df.to_dict(orient='records'))

        return dados_concatenados
    except Exception as e:
        # Se ocorrer algum erro durante a solicitação, retorna uma lista vazia
        print(f'Erro ao consultar dados para o ano {ano}: {str(e)}')
        return []

# Função para categorizar a coluna "Categoria" para Processamento
def categorizar_processamento(y):
    if y == 1:
        return 'Viníferas'
    elif y == 2:
        return 'Americanas e híbridas'
    elif y == 3:
        return 'Uvas de mesa'
    elif y == 4:
        return 'Sem classificação'

# Função para categorizar a coluna "Categoria" para Importação
def categorizar_importacao(y):
    if y == 1:
        return 'Vinhos de mesa'
    elif y == 2:
        return 'Espumantes'
    elif y == 3:
        return 'Uvas frescas'
    elif y == 4:
        return 'Uvas passas'
    elif y == 5:
        return 'Suco de uva'

# Função para categorizar a coluna "Categoria" para Exportação
def categorizar_exportacao(y):
    if y == 1:
        return 'Vinhos de mesa'
    elif y == 2:
        return 'Espumantes'
    elif y == 3:
        return 'Uvas frescas'
    elif y == 4:
        return 'Suco de uva'

# Rota para consulta
@app.route('/dados')
def consulta():
    try:
        dados_concatenados = []

        # Itera sobre os anos de 1970 a 2023
        for ano in range(1970, 2024):
            dados_ano = consultar_dados_por_ano(ano)
            dados_concatenados.extend(dados_ano)

        # Retorna os dados como JSON
        return jsonify(dados_concatenados)
    except Exception as e:
        # Se ocorrer algum erro durante a consulta, retorna uma mensagem de erro
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)