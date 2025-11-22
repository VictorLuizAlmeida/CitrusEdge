# Notificação SMS

Lambda function que envia SMS diário com o último score de previsão do modelo.

Busca a última linha da tabela `predictions1`, pega o score e envia por SMS para o número configurado.

Executa todo dia às 16:30 (horário de Brasília) via EventBridge Scheduler.

