## <b>Análise de Dados Legislativos - Câmara dos Deputados</b>

Este projeto apresenta uma solução completa de Engenharia de Dados utilizando a **Arquitetura Medallion** no **Databricks**. O objetivo principal é extrair, processar e analisar dados da API de Dados Abertos da Câmara para gerar insights estratégicos sobre a atividade parlamentar brasileira.

### <b>Tecnologias Utilizadas</b>
- **Plataforma:** Databricks Free Edition
- **Linguagens:** PySpark (Python) e Spark SQL
- **Fonte de Dados:** API REST Dados Abertos Câmara
- **Arquitetura:** Medallion (Bronze, Silver, Gold)

---
### <b>Mapeamento de Scripts e Entregas</b>

Abaixo, os scripts estão organizados por módulos funcionais, relacionando cada arquivo à sua respectiva camada e entrega solicitada.

#### <b>1. Atlas de Frentes Parlamentares</b>
Mapeamento completo das frentes ativas, seus membros e a interseção com partidos e UFs para revelar bancadas temáticas.

Entregas:<br>
    • Tabela gold de frentes com membros, partido, UF e legislatura por frente<br>
    • Identificação das frentes com maior diversidade partidária (índice de Herfindahl)<br>
    • Deputados que participam de mais frentes — perfil e temas de interesse<br>
    • Sobreposição de membros entre frentes: quem está em frentes ideologicamente opostas?<br>
    • Evolução do número de frentes por tema entre legislaturas<br>

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `1.1_frentes_ingest.txt` | 1_bronze/src/1_atlas_frentes/ |Bronze | Ingestão bruta de frentes. | Gera a tabela bronze_frentes 
| `1.1_membros_frentes_ingest.txt` | 1_bronze/src/1_atlas_frentes/ | Bronze | Ingestão bruta de membros vinculados. | Gera a tabela bronze_membros_frentes 
| `1.5_frentes_historico_ingest.txt` | 1_bronze/src/1_atlas_frentes/ | Bronze | Captura de dados da legislatura anterior (56).  | Ingestao de frentes legislatura 56 (fonte usada na 3_gold/src/1_atlas_frentes/1.5_evolucao_tematica_frentes) 
| `1.1_frentes_transform.txt` |  2_silver/src/1_atlas_frentes/ | Silver | Padronização e limpeza de dados de frentes. | Converter os tipos e estrutura referente os dados da tabela Bronze_Frentes. A tabela silver_frentes é utilizada no /3_gold/1_atlas_frentes/1.1_atlas_frentes_consolidado |
| `1.1_membros_frentes_transform.txt | 2_silver/src/1_atlas_frentes/ | Silver | Refinamento da lista de membros. | Gera a tabela silver_membros_frentes |
| `1.1_atlas_frentes_consolidado.txt` | | Gold | **Entrega:** Mapeamento completo de deputados por frente. |
| `1.2_diversidade_partidaria_frentes.txt` | | Gold | **Entrega:** Cálculo do Índice de Herfindahl para medir pluralidade. |
| `1.3_ranking_participacao_frentes.txt` | | Gold | **Entrega:** Ranking de deputados mais participativos. |
| `1.4_sobreposicao_ideologica_frentes.txt` | | Gold | **Entrega:** Identificação de parlamentares em frentes opostas. |
| `1.5_evolucao_tematica_frentes.txt` | | Gold | **Entrega:** Comparativo de temas entre legislaturas 56 e 57. |

#### <b>2. Calendário de Eventos</b>
Visão consolidada de todos os eventos (sessões, audiências, seminários) com presença de deputados e pauta.
</br></br>
Entregas:</br>
    • Tabela gold de eventos com dim_orgao, dim_tipo_evento, dim_data</br>
    • Taxa de presença por deputado e por tipo de evento ao longo do ano</br>
    • Comparativo de frequência antes e depois de períodos eleitorais</br>
    • Densidade de eventos por semana e identificação de semanas sem atividade</br>
    • View pública com calendário de eventos futuros já agendados</br>

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `2.1_eventos_ingest.txt` | | Bronze | Ingestão de eventos passados. | |
| `2.5_eventos_futuros_ingest.txt` | | Bronze | Captura de agenda futura. | |
| `2.1_fato_eventos.txt` | | Gold | **Entrega:** Tabela centralizada com dimensões de órgão, data e tipo. | |
| `2.2_taxa_presenca_calc.txt` | | Gold | **Entrega:** Taxa de presença por deputado e tipo de evento. | |
| `2.3_comparativo_eleitoral.txt` | | Gold | **Entrega:** Análise de frequência pré vs. pós início de campanha. | |
| `2.4_densidade_eventos_semanal.txt` | | Gold | **Entrega:** Identificação de semanas sem atividade legislativa. | |
| `2.5_view_calendario_publico.txt` | | Gold | **Entrega:** View pública com eventos futuros agendados. | |

#### <b>3. Correlações: Frentes vs. Partidos</b>
Análise de fidelidade e alinhamento de voto.

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `3.1a_votacoes_ingest.txt` | Bronze | Ingestão de votos nominais e orientações. |
| `3.1a_alinhamento_frente_partido.txt`| Gold | **Entrega:** Verificação de voto divergente da orientação partidária. |
| `3.1b_ranking_fidelidade_frentes.txt`| Gold | **Entrega:** Ranking de fidelidade por frente parlamentar. |

#### <b>4. Gastos (CEAP)</b>
Auditoria financeira e detecção de anomalias nos gastos parlamentares.

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `4.1_ceap_despesas_incremental.txt`| Bronze | Ingestão incremental de despesas. |
| `4.2_fato_ceap_despesas.txt` | Gold | Construção da tabela fato de gastos. |
| `4.3_score_anomalia_ceap.txt` | Gold | **Entrega:** Uso de Z-Score para detectar gastos fora da curva. |
| `4.4_ranking_fornecedores_suspeitos.txt`| Gold | **Entrega:** Ranking de fornecedores com flag de CNPJ inválido/suspeito. |
| `4.5_relatorio_mensal_partidos.txt`| Gold | **Entrega:** Top 10 gastos mensais por partido. |

#### <b>5. Auditoria de CPIs</b>
Análise de eficácia e influência em Comissões Parlamentares de Inquérito.

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `5.1_ingestao_cpis_membros.txt` | Bronze | Ingestão (Mock) de dados de CPIs. |
| `5.1_visao_consolidada_cpis.txt` | Gold | **Entrega:** Timeline consolidada e membros por fase. |
| `5.2_legislacao_derivada_cpis.txt` | Gold | **Entrega:** Identificação de Projetos de Lei derivados de CPIs. |
| `5.3_analise_duracao_cpis.txt` | Gold | **Entrega:** Lista de CPIs que excederam o prazo de 120 dias. |
| `5.4_rede_convocados_cpis.txt` | Gold | **Entrega:** Mapeamento de relações entre convocados e setor privado. |
| `5.5_produtividade_cpis.txt` | Gold | **Entrega:** Comparativo CPIs com relatório vs encerradas sem conclusão. |

#### <b>6. Presença e Engajamento</b>
Monitoramento de absenteísmo e performance individual.

| Script | Local | Camada | Resumo | Detalhes
| :---   | :---  | :---   | :---   | :---      
| `6.1_ingestao_presencas_votacoes.txt`| Bronze | Simulação de presença em votações. |
| `6.1_analise_absenteismo.txt` | Gold | Medição de ausências em sessões de votação. |
| `6.2_score_engajamento_parlamentar.txt`| Gold | **Entrega:** Score composto (Presença x Votações). |
| `6.3_padrao_ausencia_temas.txt` | Gold | **Entrega:** Identificação de deputados que faltam em temas específicos. |
| `6.4_serie_temporal_e_percentil.txt`| Gold | **Entrega:** Série temporal de engajamento mensal. |
| `6.5_relatorio_mensal_deputados.txt`| Gold | **Entrega:** Relatório com percentil de performance em relação à média. |

---

#### <b>Estrutura de Pastas</b>
O projeto segue uma organização lógica por camadas e módulos:
- `1_bronze/src/`: Scripts de ingestão (API/Mocks).
- `2_silver/src/`: Scripts de transformação e limpeza.
- `3_gold/src/`: Scripts de regras de negócio e entregas finais.
- `eda/`: Scripts utilizados para análise exploratória de dados inicial.

---

#### <b>Como Executar?</b>
1. Configure as credenciais de acesso ao Databricks Free Edition (pode usar o SSO com a conta do Google, por exemplo).
2. Execute os scripts da camada **Bronze** para popular as tabelas brutas. <b>Siga a ordem numérica.</b>
3. Processe a camada **Silver** para sanitização dos dados.
4. Execute os scripts da camada **Gold** para gerar as tabelas de análise e os relatórios finais.

