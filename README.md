## <b>Análise de Dados Legislativos - Dados Abertos da Câmara dos Deputados</b>

Este projeto apresenta uma solução completa de Engenharia de Dados utilizando a **Arquitetura Medallion** no **Databricks**. O objetivo principal é extrair, processar e analisar dados da API de Dados Abertos da Câmara para gerar insights estratégicos sobre a atividade parlamentar brasileira.

### <b>Tecnologias Utilizadas</b>
- **Plataforma:** Databricks Free Edition¹
- **Linguagens:** PySpark (Python) e Spark SQL
- **Fonte de Dados:** API REST Dados Abertos Câmara (https://dadosabertos.camara.leg.br/swagger/api.html)
- **Arquitetura:** Medallion (Bronze, Silver, Gold)

¹ Saiba mais: https://www.databricks.com/blog/introducing-databricks-free-edition
</br></br>
---

### <b>Mapeamento de Scripts e Entregas</b>

Abaixo, os scripts estão organizados por módulos funcionais, relacionando cada arquivo à sua respectiva camada e entrega solicitada.

#### <b>1. Atlas de Frentes Parlamentares</b>
Mapeamento completo das frentes ativas, seus membros e a interseção com partidos e UFs para revelar bancadas temáticas.
<br>
- Tabela gold de frentes com membros, partido, UF e legislatura por frente<br>
- Identificação das frentes com maior diversidade partidária (índice de Herfindahl)<br>
- Deputados que participam de mais frentes — perfil e temas de interesse<br>
- Sobreposição de membros entre frentes: quem está em frentes ideologicamente opostas?<br>
- Evolução do número de frentes por tema entre legislaturas<br>

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `1.1_frentes_ingest` | 1_bronze/src/1_atlas_frentes | Bronze | Ingestão bruta de frentes | Gera a tabela bronze_frentes
| `1.1_membros_frentes_ingest` | 1_bronze/src/1_atlas_frentes | Bronze | Ingestão bruta de membros vinculados | Gera a tabela bronze_membros_frentes 
| `1.5_frentes_historico_ingest` | 1_bronze/src/1_atlas_frentes | Bronze | Captura de dados da legislatura anterior (56)  | Ingestao de frentes legislatura 56 (fonte usada na 3_gold/src/1_atlas_frentes/1.5_evolucao_tematica_frentes) 
| `1.1_frentes_transform` |  2_silver/src/1_atlas_frentes | Silver | Padronização e limpeza de dados de frentes | Converter os tipos e estrutura referente os dados da tabela Bronze_Frentes. A tabela silver_frentes é utilizada no /3_gold/1_atlas_frentes/1.1_atlas_frentes_consolidado
| `1.1_membros_frentes_transform` | 2_silver/src/1_atlas_frentes | Silver | Refinamento da lista de membros | Gera a tabela silver_membros_frentes 
| `1.5_frentes_transform` | 2_silver/src/1_atlas_frentes | Silver | Padronização e limpeza de dados de frentes | Converter os tipos e estrutura referente os dados da tabela Bronze_Frentes Fonte para o script 3_gold/1_atlas_frentes/1.5_evolucao_tematica_frentes
| `1.1_atlas_frentes_consolidado` | 3_gold/src/1_atlas_frentes | Gold | Mapeamento completo de deputados por frente | Gerar tabelas de Frentes e seus membros usando como fonte tabelas dos scripts /2_silver/src/1_atlas_frentes/2_membros_frentes_transform e 1.1_frentes_transform 
| `1.2_diversidade_partidaria_frentes` | 3_gold/src/1_atlas_frentes | Gold | Cálculo do Índice de Herfindahl para medir pluralidade | Gerar tabelas de apoio para análise diversidade partidária (fonte 'gold_atlas_frentes_parlamentares' gerado pelo script 3_gold/1_atlas_frentes/1.1_atlas_frentes_consolidado)
| `1.3_ranking_participacao_frentes` | 3_gold/src/1_atlas_frentes | Gold | Ranking de deputados mais participativos | Gerar ranking de parlamentares por número de frentes (fonte 'gold_atlas_frentes_parlamentares' gerado pelo script 3_gold/1_atlas_frentes/1.1_atlas_frentes_consolidado)
| `1.4_sobreposicao_ideologica_frentes` | 3_gold/src/1_atlas_frentes | Gold | Identificação de parlamentares em frentes opostas | Gerar tabelas com sobreposição de membros entre frentes ideologicamente opostas (fonte 'gold_atlas_frentes_parlamentares' gerado pelo script 3_gold/1_atlas_frentes/1.1_atlas_frentes_consolidado)
| `1.5_evolucao_tematica_frentes` | 3_gold/src/1_atlas_frentes | Gold | Comparativo de temas entre legislaturas 56 e 57 | Evolução temática das frentes (dependência as tabelas geradas pelos scripts 2_Silver\1_atlas_frentes\1.2_frentes_transform e 1.5_frentes_historico_ingest)

#### <b>2. Calendário de Eventos</b>
Visão consolidada de todos os eventos (sessões, audiências, seminários) com presença de deputados e pauta.
</br>
- Tabela gold de eventos com dim_orgao, dim_tipo_evento, dim_data</br>
- Taxa de presença por deputado e por tipo de evento ao longo do ano</br>
- Comparativo de frequência antes e depois de períodos eleitorais</br>
- Densidade de eventos por semana e identificação de semanas sem atividade</br>
- View pública com calendário de eventos futuros já agendados</br>

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `2.1_eventos_ingest` | 1_bronze/src/2_calendario_eventos | Bronze | Ingestão de eventos passados | Dados brutos referente os eventos extraidos da API da Câmara dos Deputados 
| `2.1_orgaos_ingest` | 1_bronze/src/2_calendario_eventos | Bronze | Ingestão de orgãos da API | Ingestão de dados da API do Legislativo Brasileiro sobre órgãos legislativos (Gera a tabela bronze_orgaos)
| `2.1_referencia_tipoevento_ingest` | 1_bronze/src/2_calendario_eventos | Bronze | Ingestão de dados referentes ao tipo de evento | Script para gerar a tabela bronze_referencia_tipoevento
| `2.2_presencas_eventos_ingest` | 1_bronze/src/2_calendario_eventos | Bronze | Ingestão de dados de presença de deputados em eventos parlamentares | Script para gerar a tabela bronze_presenca_eventos
| `2.5_eventos_futuros_ingest` | 1_bronze/src/2_calendario_eventos | Bronze | Captura de agenda futura | Foco em eventos com datas posteriores a hoje (gera a tabela bronze_eventos_futuros)
| `2.1_eventos_transform` | 2_silver/src/2_calendario_eventos | Silver | Transformação de dados da tabela bronze_eventos | Gera a tabela silver_eventos
| `2.1_orgaos_transform` | 2_silver/src/2_calendario_eventos | Silver | Transformação de dados da tabela bronze_orgaos | Gera a tabela silver_orgaos
| `2.1_referencia_tipoevento_transform` | 2_silver/src/2_calendario_eventos | Silver | Transformar bronze_referencia_tipoevento | Gera a tabela silver_referencia_tipoevento
| `2.2_presenca_eventos_transform` | 2_silver/src/2_calendario_eventos | Silver | Transformar a tabela de presenças em uma tabela Silver com os tipos de eventos | Gera a tabela silver_presenca_eventos para a taxa de presença por deputado e por tipo de evento ao longo do ano
| `2.5_eventos_futuros_transform` | 2_silver/src/2_calendario_eventos | Silver | Transformação de dados da tabela bronze_eventos_futuros | Converter tipos/estrutura para ser usada na view pública a partir da tabela gerada pelo script 1_bronze/src/2_calendario_eventos/2.5_eventos_futuros_ingest
| `2.1_fato_eventos` | 3_gold/2_calendario_eventos | Gold | Tabela centralizada com dimensões de órgão, data e tipo | Script que gera a tabela gold_fato_eventos
| `2.1a_dim_orgao` | 3_gold/2_calendario_eventos | Gold | Lê a tabela silver_orgaos | Dados obtidos da tabela silver_orgaos gerados pelo script 2_silver/src/2_calendario_eventos/2.1_orgaos_transform
| `2.1b_dim_tipoevento` | 3_gold/2_calendario_eventos | Gold | Lê a tabela silver_referencia_tipoevento | Dados obtidos da tabela silver_referencia_tipoevento gerados pelo script 2_silver/src/2_calendario_eventos/2.1_referencia_tipoevento_transforma
| `2.1c_dim_data` | 3_gold/2_calendario_eventos | Gold | Geração de datas para o calendário de eventos | Script que gera a tabela gold_dim_data
| `2.2_taxa_presenca_calc` | 3_gold/src/2_calendario_eventos | Gold | Taxa de presença por deputado e tipo de evento | Calcular a taxa de presença por deputado e tipo de evento (gera a tabela gold_taxa_presenca_deputado)
| `2.3_comparativo_eleitoral` | 3_gold/src/2_calendario_eventos | Gold | Análise de frequência pré vs. pós início de campanha |  Comparativo de frequência de eventos a partir da tabela gerada pelo script 2_calendario_eventos\2.1_fato_eventos
| `2.4_densidade_eventos_semanal` | 3_gold/src/2_calendario_eventos | Gold | Identificação de semanas sem atividade legislativa | Densidade de eventos e semanas sem atividade a partir da tabela gerada pelo script 3_gold/2_calendario_eventos/2.1_fato_eventos 
| `2.5_view_calendario_publico` | 3_gold/src/2_calendario_eventos | Gold | View pública com eventos futuros agendados | View pública com calendário de eventos futuros já agendados e obtidos pela tabela do script 2_silver/src/2_calendario_eventos/2.5_eventos_futuros_transform

#### <b>3. Correlações: Frentes vs. Partidos</b>
Análise de fidelidade e alinhamento de voto. Verificar se deputados de uma mesma frente votam de forma mais alinhada do que seus colegas de partido.

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `3.1a_votacoes_ingest` | 1_bronze/src/3_correlacoes_frentes_votos | Bronze | Ingestão de votos nominais e orientações | Ingestão de dados sobre votações/presenças parlamentares. Fonte p. 3_gold/../3_correlacoes_frentes_votos/3.1a_alinhemento_frente_partido
| `3.1a_alinhamento_frente_partido`| 3_gold/src/3_correlacoes_frentes_votos | Gold | Verificação de voto divergente da orientação partidária | Retorna se o deputado votou ou não diferente da orientação do partido. Fonte das tabelas geradas na 1_bronze/..3.1a_votacoes_ingest 
| `3.1b_ranking_fidelidade_frentes`| 3_gold/src/3_correlacoes_frentes_votos | Gold | Ranking de fidelidade por frente parlamentar | Retorna ranking de fidelidade partidária por frente. Fonte gold_correlacao_frente_voto gerada pelo script 3.1a_alinhamento_frente_partido

#### <b>4. Gastos (CEAP)</b>
Auditoria financeira e detecção de anomalias nos gastos parlamentares.
</br> 
- Ingestão incremental de /deputados/{id}/despesas com controle de paginação</br>
- Tabela fato_despesas com dimensões: deputado, fornecedor, categoria, mês</br>
- Score de anomalia: z-score por categoria × estado do deputado</br>
- Ranking de fornecedores mais pagos com flags de CNPJ suspeito (consulta Receita)</br>
- Relatório mensal automatizado com top 10 gastos por partido</br>

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `4.1_ceap_despesas_incrementa`| 1_bronze/src/4_gastos_ceap | Bronze | Ingestão incremental de despesas | Gerar dados de despesas do CEAP incrementalmente fornecendo a tabela bronze_ceap_despesas
| `4.2_fato_ceap_despesas` | 3_gold/src/4_gastos_ceap | Gold | Construção da tabela fato de gastos | Gerar fato de gastos com fornecedores usando a tabela gerada pelo script 1_bronze/src/4_gastos_ceap/4.1_ceap_despesas_incremental
| `4.3_score_anomalia_ceap` | 3_gold/src/4_gastos_ceap | Gold | Uso de Z-Score para detectar gastos fora da curva | Calcula o Z-Score a partir da tabela gerada no script 3_gold/4_gastos_ceap/4.2_fato_ceap_despesas
| `4.4_ranking_fornecedores_suspeitos`| 3_gold/src/4_gastos_ceap | Gold | Ranking de fornecedores com flag de CNPJ inválido/suspeito | Gerar ranking de fornecedores mais pagos a partir da tabela gerada no script 3_gold/4_gastos_ceap/4.2_fato_ceap_despesas
| `4.5_relatorio_mensal_partidos.txt`| 3_gold/src/4_gastos_ceap | Gold | Top 10 gastos mensais por partido | Gerar relatório mensal (10 maiores gastos p/ partido) a partir da tabela gerada no script 3_gold/4_gastos_ceap/4.2_fato_ceap_despesas

#### <b>5. Auditoria de CPIs</b>
Rastreamento completo do ciclo de vida de CPIs: criação, eventos, convocados, relatório final e desfecho. Entregas esperadas
</br>
- Tabela específica de CPIs com timeline de eventos e membros por fase
- Join com proposicoes para identificar legislação derivada de cada CPI
- Análise de duração: CPIs que excederam prazo regimental e motivos
- Rede de convocados cruzada com entidades privadas (quando disponível em fontes abertas)
- Comparativo de produtividade: CPIs que geraram relatório vs. as que foram encerradas sem conclusão 

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `5.1_ingestao_cpis_membros` | 1_bronze/src/5_auditoria_cpis | Bronze | Ingestão (Mock) de dados de CPIs | Criar massa de dados para Auditoria de CPIs (Independente de API). Gera as tabelas bronze_cpis_lista e bronze_cpis_membros
| `5.1a_cpis_timeline_silver` | 2_silver/src/5_auditoria_cpis | Silver | Transforma a tabela bronze_cpis_lista | Tratamento de datas e cálculo de duração das CPIs. Fornece fonte para o script  3_gold/../5_auditoria_cpis/5.1_visao_consolidada_cpis e tambem 5.4_rede_convocados_cpis
| `5.1b_cpis_membros_silver` | 2_silver/src/5_auditoria_cpis | Silver | Limpeza e padronização dos membros das CPIs (tabela bronze_cpis_membros) | Tabela específica de CPIs com membros por fase
| `5.1_visao_consolidada_cpis` | 3_gold/src/5_auditoria_cpis | Gold | Timeline consolidada e membros por fase | Unificar timeline e membros para a visão final de auditoria. Usa tabelas originadas dos scripts /2_silver/src/5_auditoria_cpis/5.1a_cpis_timeline_silver e 5.1b_cpis_membros_silver"
| `5.2_legislacao_derivada_cpis` | 3_gold/src/5_auditoria_cpis | Gold | Identificação de Projetos de Lei derivados de CPIs | Cruzar CPIs com proposições legislativas derivadas a partir da tabela gerada na 3_gold\..\5.1_visao_consolidade_cpis para o join com proposicoes para identificar legislação derivada de cada CPI
| `5.3_analise_duracao_cpis` | 3_gold/src/5_auditoria_cpis | Gold | Análise de duração: CPIs que excederam prazo regimental | Gerar uma base consolidada com todos os CPIs e seus prazos (tabela gold_auditoria_duracao)
| `5.4_rede_convocados_cpis` | 3_gold/src/5_auditoria_cpis | Gold | Mapeamento de relações entre convocados e setor privado | Gera a tabela gold_cpis_rede_convocados
| `5.5_produtividade_cpis` | 3_gold/src/5_auditoria_cpis | Gold | Comparativo CPIs com relatório vs encerradas sem conclusão | Gera a tabela gold_cpis_comparativo_produtividade

#### <b>6. Presença e Engajamento</b>
Monitoramento de absenteísmo e performance individual.

- Desenvolver uma visão sobre junção entre eventos e votações para medir ausências em votações
- Score de engajamento composto: presença × votações × discursos × requerimentos
- Detecção de padrão de ausência: deputados que faltam mais em votações específicas
- Série temporal de engajamento com queda após eventos críticos (escândalos, reeleição)
- Relatório automático mensal para cada deputado com percentile em relação à média

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `6.1_ingestao_presencas_votacoes` | 1_bronze/src/6_monitor_presenca | Bronze | Simulação de presença em votações | Simular votações de deputados para obter tendências de alinhamento entre deputados (mesma frente). Usa como fonte a gold_atlas_frentes_parlamentares e fornece a bronze_presenca_votacoes para ser usada no script 3_gold/../6.1_analise_absentismo
| `6.3_ingestao_temas_votacoes` | 1_bronze/src/6_monitor_presenca | Bronze | Detecção de padrão de ausência: deputados x faltas em votações | Fonte para ser usada no script 3_gold/../6_monitor_presenca
| `6.1_analise_absenteismo` | 3_gold/src/6_monitor_presenca | Gold | Medição de ausências em sessões de votação | Monitora as ausencias. Usa as tabelas gold_atlas_frentes_parlamentares e bronze_presenca_votacoes (gerada pela 1_bronze..6.1_ingestao_presencas_votacoes)
| `6.2_score_engajamento_parlamentar` | 3_gold/src/6_monitor_presenca | Gold | Score (Presença x Votações) | Calcular o score de engajamento tendo como fonte a tabela gerada no script 3_gold/../6.1_analise_absenteismo
| `6.3_padrao_ausencia_temas` | 3_gold/src/6_monitor_presenca | Gold | Identificação de deputados que faltam em temas específicos | Obter o percentual de ausência por tema. Usa como fonte de dados a gold_monitor_absenteismo gerada pelo script (3_gold/src/6_monitor_presenca/6.1_analise_absenteismo) e bronze_temas_votacoes gerada pelo script (1_bronze/src/6_monitor_presenca/6.3_ingestao_temas_votacoes)
| `6.4_serie_temporal_e_percentil` | 3_gold/src/6_monitor_presenca | Gold | Série temporal de engajamento mensal | Gerar relatório mensal de engajamento usando como fonte a tabela do script /3_gold/6_monitor_presenca/6.1_analise_absenteismo
| `6.5_relatorio_mensal_deputados` | 3_gold/src/6_monitor_presenca | Gold | Relatório com percentil de performance em relação à média | erar relatório mensal (retornar percentil de cada deputado). Fonte usando a tabela do /3_gold/6_monitor_presenca/6.1_analise_absenteismo |

</br>

---

#### <b>Estrutura de Pastas</b>
O projeto segue uma organização lógica por camadas:
- `1_bronze/src/`: Scripts de ingestão (API/Mocks).
- `2_silver/src/`: Scripts de transformação e limpeza.
- `3_gold/src/`: Scripts de regras de negócio e entregas finais.
- `eda/`: Scripts utilizados para análise exploratória de dados inicial.

</br>

---

#### <b>Como Executar?</b>
1. Configure as credenciais de acesso ao Databricks Free Edition (pode usar o SSO com a conta do Google, por exemplo).
2. Execute os scripts da camada **Bronze** para popular as tabelas brutas. <b>Siga a ordem numérica.</b>
3. Processe a camada **Silver** para sanitização dos dados.
4. Execute os scripts da camada **Gold** para gerar as tabelas de análise e os relatórios finais.

