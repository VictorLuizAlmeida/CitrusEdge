# Análise Exploratória de Dados (EDA)

Este diretório contém a análise exploratória de dados realizada para o desenvolvimento do modelo de predição do sistema CitrusEdge.

## Conteúdo

- **DEV_MODELO.ipynb**: Notebook completo com toda a análise exploratória e desenvolvimento do modelo
- **eda_report.html**: Versão HTML exportada do notebook (para visualização sem executar)

## Resumo da Análise

### Dados Analisados

- **Período**: 2015-01-01 a 2025-09-18
- **Total de observações**: 93,928 registros horários
- **Variáveis meteorológicas**: temperatura, pressão, umidade, ponto de orvalho, velocidade do vento, direção do vento, precipitação, visibilidade, cobertura de nuvens

### Principais Descobertas

1. **Inspeção dos Dados**
   - Dataset completo sem valores ausentes significativos
   - Dados horários de observações meteorológicas

2. **Estatísticas Descritivas**
   - Análise completa das distribuições de todas as variáveis numéricas
   - Identificação de valores extremos e padrões

3. **Análise de Correlação**
   - Correlação de Spearman (monotonicidade)
   - Correlação de Pearson (linearidade)
   - Relações identificadas:
     - Pressão atmosférica diminui com aumento da temperatura
     - Umidade relativa diminui com aumento da temperatura
     - Ponto de orvalho correlacionado positivamente com umidade relativa

4. **Análise de Séries Temporais**
   - Análise de estacionariedade (teste de Dickey-Fuller)
   - Identificação de tendências e sazonalidades
   - Análise de ciclos diários e anuais
   - Autocorrelação das variáveis

5. **Engenharia de Features**
   - Criação de features de lag (1h, 3h, 6h, 12h, 18h, 24h, 48h, 72h)
   - Features de janelas móveis (3 dias e 15 dias)
   - Estatísticas agregadas (min, max, mean, percentis, desvio padrão)

6. **Criação do Target**
   - Variável binária `pulverizar_amanha` baseada em condições meteorológicas:
     - Vento entre 3-10 km/h nas horas 6-8
     - Umidade relativa >= 50% nas horas 6-8
     - Temperatura <= 30°C nas horas 6-8
     - Sem precipitação nas horas 6-13

7. **Desenvolvimento do Modelo**
   - Modelo: CatBoost Classifier
   - Divisão temporal: treino até 2024-01-01, validação após
   - Métricas de avaliação: ROC AUC, Accuracy, Precision, Recall, F1-Score

