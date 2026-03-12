# Sistema de Vendas (PDV)

Aplicação Flask com:
- PDV com busca de produtos, desconto, pagamentos (dinheiro/pix/cartões) e cancelamento de venda.
- Abertura/fechamento de caixa com diferença de caixa e histórico.
- Cadastro de produtos, controle e movimentação de estoque.
- Relatórios de faturamento, lucro aproximado, top produtos e estoque.
- Login administrador, histórico de vendas e reimpressão de comprovante.
- Dashboard com visão diária.

## Como rodar

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Acesse: `http://localhost:5000`

Login padrão: `admin` / `admin123`.

## Troubleshooting

Se a tela mostrar texto começando com `diff --git`, o servidor está servindo um arquivo de patch/diff em vez do template HTML.
Garanta que o arquivo `templates/cash.html` esteja com conteúdo Jinja/HTML válido e reinicie o servidor:

```bash
git pull
python app.py
```


Se abrir o site e aparecer somente o CSS (texto começando em `:root { ... }`), faça um hard refresh no navegador (`Ctrl+F5`) e confirme que está acessando `http://localhost:5000` (não `.../static/style.css`).
