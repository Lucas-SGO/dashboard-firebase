# Dashboard Firebase

Projeto web para monitoramento de ativacoes em tempo real com fonte de dados do Firebase Realtime Database ou arquivo JSON.

## Estrutura

- `site/index.html`: interface do dashboard
- `site/styles.css`: estilos
- `site/app.js`: logica da aplicacao
- `server.js`: servidor HTTP local para desenvolvimento

## Como rodar

1. Entre na pasta do projeto:
   - `cd /home/projetos/dashboard_firebase`
2. Suba o servidor:
   - `npm start`
3. Abra no navegador:
   - `http://localhost:5173`

## Fonte de dados

- Pelo painel de configuracao, informe:
  - Database URL do Firebase
  - Caminho dos dados (ex: `Ativacoes`)
  - API Key (opcional)
- Ou carregue um arquivo JSON exportado.
