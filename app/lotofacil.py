import pandas as pd
from utils.utils import Utils
import logging
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
from dash import Input, Output
import dash_bootstrap_components as dbc
from utils.spark_session_conn import SparkSessionConn
from app import *
from dash_bootstrap_templates import ThemeSwitchAIO

# ========== Styles ============ #
tab_card = {'height': '100%'}

main_config = {
    "hovermode": "x unified",
    "legend": {"yanchor": "top",
               "y": 1,
               "xanchor": "left",
               "x": 1,
               "title": {"text": None},
               "font": {"color": "white"},
               "bgcolor": "rgba(0,0,0,0.5)"},
    "margin": {"l": 10, "r": 10, "t": 10, "b": 10}
}

config_graph = {"displayModeBar": False, "showTips": False}

template_theme1 = "darkly"
template_theme2 = "solar"
url_theme1 = dbc.themes.DARKLY
url_theme2 = dbc.themes.SOLAR

tipo_jogo = 'lotofacil'
iniciais_jogo = 'LOTOFACIL'

# ===== Start SparkSession ====== #
logging.info(f'Iniciando Sessão Spark get_delta_table_{tipo_jogo}')
spark = SparkSessionConn.build_delta_minio(f'get_delta_table_{tipo_jogo}')

# Set Delta Tables
table_sorteios = f'loterias_caixa.sorteados_{tipo_jogo}'
table_ocorrencias_ordem = f'loterias_caixa.ocorrencias_dezenas_{tipo_jogo}_full'
table_ocorrencias_mun_ordem = f'loterias_caixa.ocorrencias_dezenas_{tipo_jogo}_por_municipio'
table_detalhes = f'loterias_caixa.detail_sorteios_{tipo_jogo}'
table_rateio = f'loterias_caixa.rateio_premio_{tipo_jogo}'
# Get Delta Tables
spark_sorteios = spark.read.table(table_sorteios)
spark_ocorrencias = spark.read.table(table_ocorrencias_ordem)
spark_ocorrencias_mun = spark.read.table(table_ocorrencias_mun_ordem)
spark_rateio = spark.read.table(table_rateio)
spark_detalhes = spark.read.table(table_detalhes)


# Covert Dataframe Spark to Pandas
data = spark_sorteios.collect()
df_sorteados = pd.DataFrame(data, columns=spark_sorteios.columns)
logging.info(f'Dataframe Preparado e Convertido para Pandas!')

data = spark_ocorrencias.collect()
df_ocorrencias_ordem = pd.DataFrame(data, columns=spark_ocorrencias.columns)

data = spark_ocorrencias_mun.collect()
df_ocorrencias_mun_ordem = pd.DataFrame(data, columns=spark_ocorrencias_mun.columns)

data = spark_detalhes.collect()
df_detalhes = pd.DataFrame(data, columns=spark_detalhes.columns)

data = spark_rateio.collect()
df_rateio = pd.DataFrame(data, columns=spark_rateio.columns)

logging.info('Dataframes Preparados e Convertidos para Pandas!')
# ===== Stop SparkSession ====== #
spark.stop()
logging.info(f'Stop de Sessão Spark get_delta_table_{tipo_jogo}')

# Filtros
df_rateio['NUM_SORTEIO'] = df_rateio['NUM_SORTEIO'].astype(int)
df_detalhes = df_detalhes.dropna(subset=['NUM_SORTEIO'])
df_detalhes['NUM_SORTEIO'] = df_detalhes['NUM_SORTEIO'].astype(int)
df_detalhes.count()
# Obter o último sorteio
ultimo_sorteio = df_rateio['NUM_SORTEIO'].max()
ultimo_sorteio_detalhes = df_detalhes['NUM_SORTEIO'].max()

# Filtrar para o último sorteio
df_rateio_filtrado = df_rateio[df_rateio['NUM_SORTEIO'] == ultimo_sorteio]
df_detalhes_filtrado = df_detalhes[df_detalhes['NUM_SORTEIO'] == ultimo_sorteio_detalhes]
df_rateio_filtrado['TOTAL_PAGO_FAIXA'] = df_rateio_filtrado['TOTAL_PAGO_FAIXA'].round(2)
df_rateio_filtrado['NUM_GANHADORES'] = df_rateio_filtrado['NUM_GANHADORES'].astype(str)
df_rateio_filtrado['NUM_GANHADORES'] = df_rateio_filtrado['NUM_GANHADORES'].apply(Utils.format_number)
df_rateio_filtrado['VL_PREMIO'] = df_rateio_filtrado['VL_PREMIO'].astype(str)
df_rateio_filtrado['VL_PREMIO'] = df_rateio_filtrado['VL_PREMIO'].apply(Utils.format_number)
df_rateio_filtrado['VL_PREMIO'] = 'R$ ' + df_rateio_filtrado['VL_PREMIO']
df_rateio_filtrado['TOTAL_PAGO_FAIXA'] = df_rateio_filtrado['TOTAL_PAGO_FAIXA'].astype(str)
df_rateio_filtrado['TOTAL_PAGO_FAIXA'] = df_rateio_filtrado['TOTAL_PAGO_FAIXA'].apply(Utils.format_number)
df_rateio_filtrado['TOTAL_PAGO_FAIXA'] = 'R$ ' + df_rateio_filtrado['TOTAL_PAGO_FAIXA']
df_detalhes_filtrado['VL_ACUM_PROX_CONCURSO'] = df_detalhes_filtrado['VL_ACUM_PROX_CONCURSO'].astype(str)
df_detalhes_filtrado['VL_ACUM_PROX_CONCURSO'] = df_detalhes_filtrado['VL_ACUM_PROX_CONCURSO'].apply(Utils.format_number)
df_detalhes_filtrado['VL_ACUM_PROX_CONCURSO'] = 'R$ ' + df_detalhes_filtrado['VL_ACUM_PROX_CONCURSO']
df_detalhes_filtrado['VL_ESTIM_PROX_CONCURSO'] = df_detalhes_filtrado['VL_ESTIM_PROX_CONCURSO'].astype(str)
df_detalhes_filtrado['VL_ESTIM_PROX_CONCURSO'] = df_detalhes_filtrado['VL_ESTIM_PROX_CONCURSO'].apply(Utils.format_number)
df_detalhes_filtrado['VL_ESTIM_PROX_CONCURSO'] = 'R$ ' + df_detalhes_filtrado['VL_ESTIM_PROX_CONCURSO']
df_detalhes_filtrado['VL_ARRECADADO'] = df_detalhes_filtrado['VL_ARRECADADO'].astype(str)
df_detalhes_filtrado['VL_ARRECADADO'] = df_detalhes_filtrado['VL_ARRECADADO'].apply(Utils.format_number)
df_detalhes_filtrado['VL_ARRECADADO'] = 'R$ ' + df_detalhes_filtrado['VL_ARRECADADO']
df_sorteados['DT_HR_PROCESSAMENTO'] = (
    pd.to_datetime(df_sorteados['DT_HR_PROCESSAMENTO']).dt.strftime('%d/%m/%Y %H:%M'))

df_detalhes_filtrado['DT_APURACAO'] = (pd.to_datetime(df_detalhes_filtrado['DT_APURACAO']).dt.strftime('%d/%m/%Y'))
df_detalhes_filtrado['DT_PROX_CONC'] = (pd.to_datetime(df_detalhes_filtrado['DT_PROX_CONC']).dt.strftime('%d/%m/%Y'))

municipios = df_sorteados['MUNICIPIO_SORTEIO'].unique()
estados = df_sorteados['UF'].unique()

# =========  Layout  =========== #


app.layout = dbc.Container(children=[
    # Armazenamento de dataset
    # dcc.Store(id='dataset', data=df_store),
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Legend(f"Análise {iniciais_jogo}")
                        ], sm=8),
                        dbc.Col([
                            html.Img(id="logo", src=app.get_asset_url("logo_dark.png"), height=80),
                        ], sm=4, align="center")
                    ]),
                    dbc.Row([
                        dbc.Col([
                            ThemeSwitchAIO(aio_id="theme", themes=[url_theme1, url_theme2]),
                            html.Legend("Mundo dos Dados BR")
                        ])
                    ], style={'margin-top': '10px'}),
                    dbc.Row([
                        dbc.Button("Visite o Site", href="https://loterias.caixa.gov.br/Paginas/Lotofacil.aspx", target="_blank")
                    ], style={'margin-top': '10px'}),
                    dbc.Row([
                        html.H4('Data de Atualização', className='card-title'),
                        html.P(df_sorteados['DT_HR_PROCESSAMENTO'].iloc[0], className='card-text'),
                    ], style={'margin-top': '10px'}),
                ])
            ], style=tab_card)
        ], sm=4, lg=2),
        dbc.Col([

            dbc.Card([
                dbc.CardBody([
                    html.H4('Último Sorteio', className='card-title'),
                    html.P(f'Número: {ultimo_sorteio}', className='card-text'),
                    html.P(df_detalhes_filtrado['DT_APURACAO'].iloc[0], className='card-text'),
                ])
            ], style={'margin-bottom': '10px'}),
            dbc.Card([
                dbc.CardBody([
                    html.H4('Próximo Sorteio', className='card-title'),
                    html.P(f'Número: {ultimo_sorteio + 1}', className='card-text'),
                    html.P(df_detalhes_filtrado['DT_PROX_CONC'].iloc[0], className='card-text'),
                ])
            ], style={'margin-bottom': '10px'}),
            dbc.Card([
                dbc.CardBody([
                    html.H4('Valor Estimado Próximo Consurso', className='card-title'),
                    html.P(df_detalhes_filtrado['VL_ESTIM_PROX_CONCURSO'], className='card-text'),
                ])
            ], style={'margin-bottom': '10px'})
        ], sm=4, lg=2),
        dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(desc_faixa, className='card-title'),
                            html.P(f'Número de Ganhadores: {num_ganhadores}', className='card-text'),
                            html.P(f'Valor do Prêmio: {vl_premio}', className='card-text'),
                            html.P(f'Total Pago: {"ACUMULOU" if num_ganhadores == 0 else vl_faixa}',
                                   className='card-text'),
                        ]),
                    ], style={'margin-bottom': '10px'})
                    for desc_faixa, num_ganhadores, vl_premio, vl_faixa in zip(df_rateio_filtrado['DESC_FAIXA'],
                                                                               df_rateio_filtrado['NUM_GANHADORES'],
                                                                               df_rateio_filtrado['VL_PREMIO'],
                                                                               df_rateio_filtrado['TOTAL_PAGO_FAIXA'])
                    if desc_faixa == '15 acertos'
                ] + [
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(desc_faixa, className='card-title'),
                            html.P(f'Número de Ganhadores: {num_ganhadores}', className='card-text'),
                            html.P(f'Valor do Prêmio: {vl_premio}', className='card-text'),
                            html.P(f'Total Pago: {vl_faixa}', className='card-text'),
                        ])
                    ], style={'margin-bottom': '10px'})
                    for desc_faixa, num_ganhadores, vl_premio, vl_faixa in zip(df_rateio_filtrado['DESC_FAIXA'],
                                                                               df_rateio_filtrado['NUM_GANHADORES'],
                                                                               df_rateio_filtrado['VL_PREMIO'],
                                                                               df_rateio_filtrado['TOTAL_PAGO_FAIXA'])
                    if desc_faixa == '11 acertos'
                ], sm=4, lg=2),
        dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(desc_faixa, className='card-title'),
                            html.P(f'Número de Ganhadores: {num_ganhadores}', className='card-text'),
                            html.P(f'Valor do Prêmio: {vl_premio}', className='card-text'),
                            html.P(f'Total Pago: {vl_faixa}', className='card-text'),
                        ])
                    ], style={'margin-bottom': '10px'})
                    for desc_faixa, num_ganhadores, vl_premio, vl_faixa in zip(df_rateio_filtrado['DESC_FAIXA'],
                                                                               df_rateio_filtrado['NUM_GANHADORES'],
                                                                               df_rateio_filtrado['VL_PREMIO'],
                                                                               df_rateio_filtrado['TOTAL_PAGO_FAIXA'])
                    if desc_faixa == '14 acertos'
                ] + [
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Valor Acumulado Próximo Concurso", className='card-title'),
                            html.P(df_detalhes_filtrado['VL_ACUM_PROX_CONCURSO'], className='card-text'),
                        ])
                    ], style={'margin-bottom': '10px'})
                ], sm=4, lg=2),
        dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(desc_faixa, className='card-title'),
                            html.P(f'Número de Ganhadores: {num_ganhadores}', className='card-text'),
                            html.P(f'Valor do Prêmio: {vl_premio}', className='card-text'),
                            html.P(f'Total Pago: {vl_faixa}', className='card-text'),
                        ])
                    ], style={'margin-bottom': '10px'})
                    for desc_faixa, num_ganhadores, vl_premio, vl_faixa in zip(df_rateio_filtrado['DESC_FAIXA'],
                                                                               df_rateio_filtrado['NUM_GANHADORES'],
                                                                               df_rateio_filtrado['VL_PREMIO'],
                                                                               df_rateio_filtrado['TOTAL_PAGO_FAIXA'])
                    if desc_faixa == '13 acertos'
                ] + [
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Valor Total Arrecadado", className='card-title'),
                            html.P(df_detalhes_filtrado['VL_ARRECADADO'], className='card-text'),
                        ])
                    ], style={'margin-bottom': '10px'})
                ], sm=4, lg=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(desc_faixa, className='card-title'),
                    html.P(f'Número de Ganhadores: {num_ganhadores}', className='card-text'),
                    html.P(f'Valor do Prêmio: {vl_premio}', className='card-text'),
                    html.P(f'Total Pago: {vl_faixa}', className='card-text'),
                ])
            ], style={'margin-bottom': '10px'})
            for desc_faixa, num_ganhadores, vl_premio, vl_faixa in zip(df_rateio_filtrado['DESC_FAIXA'],
                                                                       df_rateio_filtrado['NUM_GANHADORES'],
                                                                       df_rateio_filtrado['VL_PREMIO'],
                                                                       df_rateio_filtrado['TOTAL_PAGO_FAIXA'])
            if desc_faixa == '12 acertos'
        ] + [
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Legend(f"Fonte: Caixa Econômica Federal")
                        ], sm=8),
                        dbc.Col([
                            html.Img(id="logo_caixa", src=app.get_asset_url("loterias_logo.png"), height=60),
                        ], sm=4, align="center")
                    ], style={'margin-top': '10px'})
                ])
            ], style={'margin-bottom': '10px'})
        ], sm=4, lg=2),

    ], className='g-2 my-auto', style={'margin-top': '10px'}),
    # Layout
    dbc.Tabs([
        dbc.Tab(label="INFORMAÇÕES DE VALORES PAGOS", children=[
            html.Div("INFORMAÇÕES DE VALORES PAGOS")]),
        dbc.Tab(label="FREQUÊNCIAS E INFORMAÇÕES", children=[
            html.Div([
                # Adicione as colunas para os gráficos aqui
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row(
                                    dbc.Col(
                                        html.Legend('Frequência dos Números Sorteados')
                                    )
                                ),
                                dbc.Row([
                                    dbc.Col([
                                        dcc.Graph(id='graph3', className='dbc', config=config_graph)
                                    ], sm=30, md=15),
                                ])
                            ])
                        ], style=tab_card)

                    ])
                ], className='g-2 my-auto', style={'margin-top': '10px'}),
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row(
                                    dbc.Col(
                                        html.Legend('Top 15 Mais Sorteados')
                                    )
                                ),
                                dbc.Row([
                                    dbc.Col([
                                        dcc.Graph(id='graph_top20', className='dbc', config=config_graph)
                                    ], sm=30, md=15),
                                ])
                            ])
                        ], style=tab_card)
                    ])
                ], className='g-2 my-auto', style={'margin-top': '10px'}),
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row(
                                    dbc.Col(
                                        html.Legend('Mais Soteados por Municipio')
                                    )
                                ),
                                dbc.Row([
                                    dcc.Dropdown(
                                        id='filtro-dropdown',
                                        options=[{'label': i, 'value': i} for i in municipios] + [
                                            {'label': i, 'value': i} for i in estados],
                                        value='SÃO PAULO'  # valor inicial
                                    ),
                                    dbc.Col(
                                        dcc.Graph(id='graph4', className='dbc', config=config_graph)
                                    )
                                ])
                            ])
                        ], style=tab_card)
                    ]),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row(
                                    dbc.Col(
                                        html.Legend('Mais Soteados por UF')
                                    )
                                ),
                                dbc.Row([
                                    dcc.Dropdown(
                                        id='filtro-dropdown2',
                                        options=[{'label': i, 'value': i} for i in estados],
                                        value='SP'  # valor inicial
                                    ),
                                    dbc.Col(
                                        dcc.Graph(id='graph5', className='dbc', config=config_graph)
                                    )
                                ])
                            ])
                        ], style=tab_card)
                    ])
                ], className='g-2 my-auto', style={'margin-top': '10px'}),

                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row(
                                    dbc.Col(
                                        html.Legend('Top 15 Mais Soteados por Municipio')
                                    )
                                ),
                                dbc.Row([
                                    dcc.Dropdown(
                                        id='filtro-dropdown_top_2',
                                        options=[{'label': i, 'value': i} for i in municipios],
                                        value='SÃO PAULO'  # valor inicial
                                    ),
                                    dbc.Col(
                                        dcc.Graph(id='graph_top20_3', className='dbc', config=config_graph)
                                    )
                                ])
                            ])
                        ], style=tab_card)
                    ]),
                    dbc.Col([
                        dbc.Card([
                            dbc.CardBody([
                                dbc.Row(
                                    dbc.Col(
                                        html.Legend('Top 15 Mais Soteados por UF')
                                    )
                                ),
                                dbc.Row([
                                    dcc.Dropdown(
                                        id='filtro-dropdown_top_1',
                                        options=[{'label': i, 'value': i} for i in estados],
                                        value='SP'  # valor inicial
                                    ),
                                    dbc.Col(
                                        dcc.Graph(id='graph_top20_2', className='dbc', config=config_graph)
                                    )
                                ])
                            ])
                        ], style=tab_card)
                    ])
                ], className='g-2 my-auto', style={'margin-top': '10px'}),
            ])
        ]),
        dbc.Tab(label="POR SORTEIO", children=[
            html.Div("POR SORTEIO"
                     )
        ]),
        dbc.Tab(label="MACHINE LEARNING", children=[
            html.Div("MACHINE LEARNING"
                     )
        ])
    ])
], fluid=True, style={'height': '100vh'})


# ======== Callbacks ========== #
# Graph 3
@app.callback(
    Output('graph3', 'figure'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def graph3(toggle):
    template = template_theme1 if toggle else template_theme2
    numeros_sorteados = []

    # Adicione os números de cada coluna BOLA_X à lista
    for i in range(1, 16):
        numeros_sorteados.extend(df_sorteados[f'BOLA_{i}'])

    # Crie um DataFrame a partir da lista de números sorteados
    df_numeros = pd.DataFrame(numeros_sorteados, columns=['Numero'])

    # Calcule a frequência de cada número
    df_frequencia = df_numeros['Numero'].value_counts().reset_index()
    df_frequencia.columns = ['Numero', 'Frequencia']

    # Ordene o DataFrame pela coluna 'Frequencia' em ordem decrescente
    df_frequencia = df_frequencia.sort_values('Frequencia', ascending=False)

    # Adicione uma nova coluna 'Ranking' que representa a ordem original dos números
    df_frequencia['Ranking'] = range(1, len(df_frequencia) + 1)

    # Crie um gráfico de barras com a frequência de cada número
    fig = px.bar(df_frequencia, x='Ranking', y='Frequencia',
                 color='Frequencia',  # Defina a cor das barras pela frequência
                 color_continuous_scale=['grey', 'darkmagenta'])  # Defina a escala de cores do azul ao vermelho

    fig.update_layout(
        main_config,
        height=180,
        template=template,
        xaxis=dict(
            tickmode='array',  # use 'array' tick mode to show every number
            tickvals=df_frequencia['Ranking'].values,  # show a tick for every number in the Top 15
            ticktext=df_frequencia['Numero'].values  # label the ticks with the original numbers
        )
    )

    return fig


@app.callback(
    Output('graph_top20', 'figure'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def graph_top20(toggle):
    template = template_theme1 if toggle else template_theme2
    numeros_sorteados = []

    # Adicione os números de cada coluna BOLA_X à lista
    for i in range(1, 16):
        numeros_sorteados.extend(df_sorteados[f'BOLA_{i}'])

    # Crie um DataFrame a partir da lista de números sorteados
    df_numeros = pd.DataFrame(numeros_sorteados, columns=['Numero'])

    # Calcule a frequência de cada número
    df_frequencia = df_numeros['Numero'].value_counts().reset_index()
    df_frequencia.columns = ['Numero', 'Frequencia']

    # Ordene o DataFrame pela coluna 'Frequencia' em ordem decrescente e pegue os 20 primeiros
    df_frequencia = df_frequencia.sort_values('Frequencia', ascending=False).head(15)

    # Adicione uma nova coluna 'Ranking' que representa a ordem original dos números
    df_frequencia['Ranking'] = range(1, len(df_frequencia) + 1)

    # Crie um gráfico de barras com a frequência dos 20 números mais sorteados
    fig = px.bar(df_frequencia, x='Ranking', y='Frequencia',
                 color='Frequencia',  # Defina a cor das barras pela frequência
                 color_continuous_scale=['grey', 'darkmagenta'])  # Defina a escala de cores do azul ao vermelho

    fig.update_layout(
        main_config,
        height=180,
        template=template,
        xaxis=dict(
            tickmode='array',  # use 'array' tick mode to show every number
            tickvals=df_frequencia['Ranking'].values,  # show a tick for every number in the Top 15
            ticktext=df_frequencia['Numero'].values  # label the ticks with the original numbers
        )
    )

    return fig


@app.callback(
    Output('graph4', 'figure'),
    [Input('filtro-dropdown', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value")]
)
def graph4(filtro_valor, toggle):
    template = template_theme1 if toggle else template_theme2
    numeros_sorteados = []

    # Filtrar o dataframe pelo valor selecionado no dropdown
    df_filtrado = df_sorteados[
        (df_sorteados['MUNICIPIO_SORTEIO'] == filtro_valor) | (df_sorteados['UF'] == filtro_valor)]

    # Adicione os números de cada coluna BOLA_X à lista
    for i in range(1, 16):
        numeros_sorteados.extend(df_filtrado[f'BOLA_{i}'])

    # Crie um DataFrame a partir da lista de números sorteados
    df_numeros = pd.DataFrame(numeros_sorteados, columns=['Numero'])

    # Calcule a frequência de cada número
    df_frequencia = df_numeros['Numero'].value_counts().reset_index()
    df_frequencia.columns = ['Numero', 'Frequencia']

    # Ordene o DataFrame pela coluna 'Frequencia' em ordem decrescente
    df_frequencia = df_frequencia.sort_values('Frequencia', ascending=False)

    # Adicione uma nova coluna 'Ranking' que representa a ordem original dos números
    df_frequencia['Ranking'] = range(1, len(df_frequencia) + 1)

    # Crie um gráfico de barras com a frequência de cada número
    fig = px.bar(df_frequencia, x='Ranking', y='Frequencia',
                 color='Frequencia',  # Defina a cor das barras pela frequência
                 color_continuous_scale=['grey', 'darkmagenta'])  # Defina a escala de cores do azul ao vermelho

    fig.update_layout(
        main_config,
        height=180,
        template=template,
        xaxis=dict(
            tickmode='array',  # use 'array' tick mode to show every number
            tickvals=df_frequencia['Ranking'].values,  # show a tick for every number in the Top 15
            ticktext=df_frequencia['Numero'].values  # label the ticks with the original numbers
        )
    )

    return fig


@app.callback(
    Output('graph5', 'figure'),
    [Input('filtro-dropdown2', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value")]
)
def graph5(uf, toggle):
    template = template_theme1 if toggle else template_theme2
    numeros_sorteados = []

    # Filtrar o dataframe pelo valor selecionado no dropdown
    df_filtrado = df_sorteados[df_sorteados['UF'] == uf]

    # Adicione os números de cada coluna BOLA_X à lista
    for i in range(1, 16):
        numeros_sorteados.extend(df_filtrado[f'BOLA_{i}'])

    # Crie um DataFrame a partir da lista de números sorteados
    df_numeros = pd.DataFrame(numeros_sorteados, columns=['Numero'])

    # Calcule a frequência de cada número
    df_frequencia = df_numeros['Numero'].value_counts().reset_index()
    df_frequencia.columns = ['Numero', 'Frequencia']

    # Ordene o DataFrame pela coluna 'Frequencia' em ordem decrescente
    df_frequencia = df_frequencia.sort_values('Frequencia', ascending=False)

    # Adicione uma nova coluna 'Ranking' que representa a ordem original dos números
    df_frequencia['Ranking'] = range(1, len(df_frequencia) + 1)

    # Crie um gráfico de barras com a frequência de cada número
    fig = px.bar(df_frequencia, x='Ranking', y='Frequencia',
                 color='Frequencia',  # Defina a cor das barras pela frequência
                 color_continuous_scale=['grey', 'darkmagenta'])  # Defina a escala de cores do azul ao vermelho

    fig.update_layout(
        main_config,
        height=180,
        template=template,
        xaxis=dict(
            tickmode='array',  # use 'array' tick mode to show every number
            tickvals=df_frequencia['Ranking'].values,  # show a tick for every number in the Top 15
            ticktext=df_frequencia['Numero'].values  # label the ticks with the original numbers
        )
    )

    return fig


@app.callback(
    Output('graph_top20_2', 'figure'),
    [Input('filtro-dropdown_top_1', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value")]
)
def graph_top20_2(uf, toggle):
    template = template_theme1 if toggle else template_theme2
    numeros_sorteados = []

    # Filtrar o dataframe pelo valor selecionado no dropdown
    df_filtrado = df_sorteados[df_sorteados['UF'] == uf]

    # Adicione os números de cada coluna BOLA_X à lista
    for i in range(1, 16):
        numeros_sorteados.extend(df_filtrado[f'BOLA_{i}'])

    # Crie um DataFrame a partir da lista de números sorteados
    df_numeros = pd.DataFrame(numeros_sorteados, columns=['Numero'])

    # Calcule a frequência de cada número
    df_frequencia = df_numeros['Numero'].value_counts().reset_index()
    df_frequencia.columns = ['Numero', 'Frequencia']

    # Ordene o DataFrame pela coluna 'Frequencia' em ordem decrescente e pegue os 20 primeiros
    df_frequencia = df_frequencia.sort_values('Frequencia', ascending=False).head(15)

    # Adicione uma nova coluna 'Ranking' que representa a ordem original dos números
    df_frequencia['Ranking'] = range(1, len(df_frequencia) + 1)

    # Crie um gráfico de barras com a frequência dos 20 números mais sorteados
    fig = px.bar(df_frequencia, x='Ranking', y='Frequencia',
                 color='Frequencia',  # Defina a cor das barras pela frequência
                 color_continuous_scale=['grey', 'darkmagenta'])  # Defina a escala de cores do azul ao vermelho

    fig.update_layout(
        main_config,
        height=180,
        template=template,
        xaxis=dict(
            tickmode='array',  # use 'array' tick mode to show every number
            tickvals=df_frequencia['Ranking'].values,  # show a tick for every number in the Top 15
            ticktext=df_frequencia['Numero'].values  # label the ticks with the original numbers
        )
    )

    return fig


@app.callback(
    Output('graph_top20_3', 'figure'),
    [Input('filtro-dropdown_top_2', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value")]
)
def graph_top20_3(municipio, toggle):
    template = template_theme1 if toggle else template_theme2
    numeros_sorteados = []

    # Filtrar o dataframe pelo valor selecionado no dropdown
    df_filtrado = df_sorteados[df_sorteados['MUNICIPIO_SORTEIO'] == municipio]

    # Adicione os números de cada coluna BOLA_X à lista
    for i in range(1, 16):
        numeros_sorteados.extend(df_filtrado[f'BOLA_{i}'])

    # Crie um DataFrame a partir da lista de números sorteados
    df_numeros = pd.DataFrame(numeros_sorteados, columns=['Numero'])

    # Calcule a frequência de cada número
    df_frequencia = df_numeros['Numero'].value_counts().reset_index()
    df_frequencia.columns = ['Numero', 'Frequencia']

    # Ordene o DataFrame pela coluna 'Frequencia' em ordem decrescente e pegue os 20 primeiros
    df_frequencia = df_frequencia.sort_values('Frequencia', ascending=False).head(15)

    # Adicione uma nova coluna 'Ranking' que representa a ordem original dos números
    df_frequencia['Ranking'] = range(1, len(df_frequencia) + 1)

    # Crie um gráfico de barras com a frequência dos 20 números mais sorteados
    fig = px.bar(df_frequencia, x='Ranking', y='Frequencia',
                 color='Frequencia',  # Defina a cor das barras pela frequência
                 color_continuous_scale=['grey', 'darkmagenta'])  # Defina a escala de cores do azul ao vermelho

    fig.update_layout(
        main_config,
        height=180,
        template=template,
        xaxis=dict(
            tickmode='array',  # use 'array' tick mode to show every number
            tickvals=df_frequencia['Ranking'].values,  # show a tick for every number in the Top 15
            ticktext=df_frequencia['Numero'].values  # label the ticks with the original numbers
        )
    )

    return fig


# Run server
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8051)
