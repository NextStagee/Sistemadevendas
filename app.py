from __future__ import annotations

import csv
import io
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from flask import Flask, Response, flash, g, redirect, render_template, request, session, url_for

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "pdv.db"

app = Flask(__name__)
app.secret_key = "dev-secret-change-me"
app.config["LOGO_PATH"] = "img/logo.png"
app.config["FAVICON_PATH"] = "img/favicon.png"

PAYMENT_METHODS = ["DINHEIRO", "PIX", "CARTAO_DEBITO", "CARTAO_CREDITO"]
LOW_STOCK_THRESHOLD = 5


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_: Any) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'ADMIN'
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            cost_price REAL NOT NULL,
            sale_price REAL NOT NULL,
            stock_qty INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cash_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            opening_amount REAL NOT NULL,
            closing_amount_reported REAL,
            expected_cash REAL,
            cash_difference REAL,
            total_sales REAL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'OPEN'
        );

        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            subtotal REAL NOT NULL,
            discount REAL NOT NULL,
            total REAL NOT NULL,
            payment_method TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            cancellation_reason TEXT,
            cash_session_id INTEGER,
            FOREIGN KEY(cash_session_id) REFERENCES cash_sessions(id)
        );

        CREATE TABLE IF NOT EXISTS sale_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            total_price REAL NOT NULL,
            FOREIGN KEY(sale_id) REFERENCES sales(id) ON DELETE CASCADE,
            FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS stock_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            movement_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS monthly_closures (
            month_key TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'ATIVO',
            closed_at TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS open_tabs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            client_name TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            discount REAL NOT NULL DEFAULT 0,
            total_debt REAL NOT NULL,
            note TEXT,
            FOREIGN KEY(product_id) REFERENCES products(id)
        );
        """
    )
    db.execute(
        "INSERT OR IGNORE INTO users (username, password, role) VALUES ('admin','admin123','ADMIN')"
    )
    db.commit()
    db.close()


def require_login():
    if "user_id" not in session:
        flash("Faça login para acessar o sistema.", "warning")
        return redirect(url_for("login"))
    return None


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


STATUS_LABELS = {
    "ACTIVE": "ATIVA",
    "CANCELLED": "CANCELADA",
    "OPEN": "ABERTO",
    "CLOSED": "FECHADO",
}

MOVEMENT_LABELS = {
    "ENTRY": "ENTRADA",
    "ADJUST": "AJUSTE",
    "SALE": "VENDA",
    "CANCEL": "CANCELAMENTO",
}


def fmt_dt(value: str | None) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%d/%m %H:%M")
    except ValueError:
        return value


def label_status(value: str | None) -> str:
    if not value:
        return "-"
    return STATUS_LABELS.get(value, value)


def label_movement(value: str | None) -> str:
    if not value:
        return "-"
    return MOVEMENT_LABELS.get(value, value)


def is_month_closed(db: sqlite3.Connection, month_key: str) -> bool:
    row = db.execute("SELECT status FROM monthly_closures WHERE month_key = ?", (month_key,)).fetchone()
    return bool(row and row["status"] == "FECHADO")


def parse_money(value: str | None) -> float:
    raw = (value or "0").strip().replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    return float(raw or 0)


app.jinja_env.filters["fmt_dt"] = fmt_dt
app.jinja_env.filters["label_status"] = label_status
app.jinja_env.filters["label_movement"] = label_movement


@app.context_processor
def inject_branding() -> dict[str, str]:
    return {
        "logo_path": app.config["LOGO_PATH"],
        "favicon_path": app.config["FAVICON_PATH"],
        "asset_version": int(datetime.now().timestamp()) // 3600,
    }


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        db = get_db()
        user = db.execute(
            "SELECT * FROM users WHERE username = ? AND password = ?",
            (username, password),
        ).fetchone()
        if user:
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            return redirect(url_for("dashboard"))
        flash("Usuário ou senha inválidos.", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))




@app.post("/admin/reset-data")
def reset_data():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp

    db = get_db()
    db.execute("DELETE FROM sale_items")
    db.execute("DELETE FROM sales")
    db.execute("DELETE FROM cash_sessions")
    db.execute(
        "DELETE FROM sqlite_sequence WHERE name IN ('sale_items','sales','cash_sessions')"
    )
    db.commit()
    flash("Dados de caixa e vendas foram apagados com sucesso.", "warning")
    return redirect(url_for("dashboard"))


@app.route("/")
def dashboard():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp

    db = get_db()
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    day_stats = db.execute(
        """
        SELECT COALESCE(SUM(total),0) AS faturamento, COUNT(*) AS vendas
        FROM sales
        WHERE status = 'ACTIVE' AND created_at >= ?
        """,
        (start,),
    ).fetchone()
    low_stock = db.execute(
        "SELECT * FROM products WHERE stock_qty <= ? ORDER BY stock_qty ASC",
        (LOW_STOCK_THRESHOLD,),
    ).fetchall()
    return render_template("dashboard.html", day_stats=day_stats, low_stock=low_stock)


@app.route("/products", methods=["GET", "POST"])
def products():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()

    if request.method == "POST":
        db.execute(
            """
            INSERT INTO products (name, category, cost_price, sale_price, stock_qty, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["name"],
                request.form["category"],
                float(request.form["cost_price"]),
                float(request.form["sale_price"]),
                int(request.form["stock_qty"]),
                now_iso(),
            ),
        )
        db.commit()
        flash("Produto cadastrado com sucesso.", "success")
        return redirect(url_for("products"))

    rows = db.execute("SELECT * FROM products ORDER BY name").fetchall()
    return render_template("products.html", products=rows)


@app.post("/products/<int:product_id>/update")
def product_update(product_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    db.execute(
        """
        UPDATE products
        SET name = ?, category = ?, cost_price = ?, sale_price = ?, stock_qty = ?
        WHERE id = ?
        """,
        (
            request.form["name"],
            request.form["category"],
            float(request.form["cost_price"]),
            float(request.form["sale_price"]),
            int(request.form["stock_qty"]),
            product_id,
        ),
    )
    db.commit()
    flash("Produto atualizado com sucesso.", "success")
    return redirect(url_for("products"))


@app.post("/products/<int:product_id>/delete")
def product_delete(product_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    used = db.execute("SELECT 1 FROM sale_items WHERE product_id = ? LIMIT 1", (product_id,)).fetchone()
    if used:
        flash("Produto já possui vendas registradas e não pode ser removido.", "danger")
        return redirect(url_for("products"))
    db.execute("DELETE FROM stock_movements WHERE product_id = ?", (product_id,))
    db.execute("DELETE FROM products WHERE id = ?", (product_id,))
    db.commit()
    flash("Produto removido com sucesso.", "warning")
    return redirect(url_for("products"))


@app.post("/stock/entry")
def stock_entry():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    pid = int(request.form["product_id"])
    qty = int(request.form["quantity"])
    note = request.form.get("note", "Entrada de mercadoria")
    db.execute("UPDATE products SET stock_qty = stock_qty + ? WHERE id = ?", (qty, pid))
    db.execute(
        "INSERT INTO stock_movements (product_id, movement_type, quantity, note, created_at) VALUES (?, 'ENTRY', ?, ?, ?)",
        (pid, qty, note, now_iso()),
    )
    db.commit()
    flash("Entrada de estoque registrada.", "success")
    return redirect(url_for("stock"))


@app.post("/stock/adjust")
def stock_adjust():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    pid = int(request.form["product_id"])
    qty = int(request.form["quantity"])
    note = request.form.get("note", "Ajuste manual")

    db.execute("UPDATE products SET stock_qty = stock_qty + ? WHERE id = ?", (qty, pid))

    db.execute(
        "INSERT INTO stock_movements (product_id, movement_type, quantity, note, created_at) VALUES (?, 'ADJUST', ?, ?, ?)",
        (pid, qty, note, now_iso()),
    )
    db.commit()
    flash("Ajuste de estoque registrado.", "success")
    return redirect(url_for("stock"))


@app.route("/stock")
def stock():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    products = db.execute("SELECT * FROM products ORDER BY name").fetchall()
    movements = db.execute(
        """
        SELECT sm.*, p.name AS product_name FROM stock_movements sm
        JOIN products p ON p.id = sm.product_id
        ORDER BY sm.created_at DESC LIMIT 50
        """
    ).fetchall()
    return render_template("stock.html", products=products, movements=movements, low_threshold=LOW_STOCK_THRESHOLD)


@app.route("/pdv", methods=["GET", "POST"])
def pdv():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()

    if request.method == "POST":
        payment_method = request.form["payment_method"]
        discount = float(request.form.get("discount", 0) or 0)
        ids = request.form.getlist("product_id")
        quantities = request.form.getlist("quantity")

        session_row = db.execute("SELECT * FROM cash_sessions WHERE status = 'OPEN' ORDER BY id DESC LIMIT 1").fetchone()
        if not session_row:
            flash("Abra o caixa antes de registrar vendas.", "danger")
            return redirect(url_for("cash"))

        month_key_now = datetime.now().strftime("%Y-%m")
        if is_month_closed(db, month_key_now):
            flash(f"O mês {month_key_now} está fechado para vendas.", "danger")
            return redirect(url_for("monthly_management", month=month_key_now))

        items = []
        subtotal = 0.0
        for pid_raw, qty_raw in zip(ids, quantities):
            pid = int(pid_raw)
            qty = int(qty_raw)
            product = db.execute("SELECT * FROM products WHERE id = ?", (pid,)).fetchone()
            if not product or qty <= 0:
                continue
            if product["stock_qty"] == 0:
                flash(f"Produto {product['name']} está zerado no estoque e foi ignorado.", "warning")
                continue
            if product["stock_qty"] < qty:
                flash(f"Estoque insuficiente para {product['name']}", "danger")
                return redirect(url_for("pdv"))
            line_total = qty * product["sale_price"]
            subtotal += line_total
            items.append((product, qty, line_total))

        if not items:
            flash("Selecione ao menos um item para venda.", "warning")
            return redirect(url_for("pdv"))

        total = max(subtotal - discount, 0)
        cur = db.execute(
            """
            INSERT INTO sales (created_at, subtotal, discount, total, payment_method, cash_session_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (now_iso(), subtotal, discount, total, payment_method, session_row["id"]),
        )
        sale_id = cur.lastrowid

        for product, qty, line_total in items:
            db.execute(
                """
                INSERT INTO sale_items (sale_id, product_id, product_name, quantity, unit_price, total_price)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (sale_id, product["id"], product["name"], qty, product["sale_price"], line_total),
            )
            db.execute(
                "UPDATE products SET stock_qty = stock_qty - ? WHERE id = ?",
                (qty, product["id"]),
            )
            db.execute(
                "INSERT INTO stock_movements (product_id, movement_type, quantity, note, created_at) VALUES (?, 'SALE', ?, ?, ?)",
                (product["id"], -qty, f"Venda #{sale_id}", now_iso()),
            )

        db.execute(
            "UPDATE cash_sessions SET total_sales = total_sales + ? WHERE id = ?",
            (total, session_row["id"]),
        )
        db.commit()
        flash(f"Venda #{sale_id} registrada com sucesso.", "success")
        return redirect(url_for("sale_receipt", sale_id=sale_id))

    query = request.args.get("q", "")
    if query:
        products = db.execute(
            "SELECT * FROM products WHERE name LIKE ? ORDER BY name",
            (f"%{query}%",),
        ).fetchall()
    else:
        products = db.execute("SELECT * FROM products ORDER BY name LIMIT 100").fetchall()

    return render_template("pdv.html", products=products, payment_methods=PAYMENT_METHODS, query=query)


@app.post("/sales/<int:sale_id>/cancel")
def cancel_sale(sale_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    reason = request.form.get("reason", "Cancelamento manual")
    db = get_db()
    sale = db.execute("SELECT * FROM sales WHERE id = ?", (sale_id,)).fetchone()
    if not sale or sale["status"] == "CANCELLED":
        flash("Venda não encontrada ou já cancelada.", "warning")
        return redirect(url_for("sales_history"))

    items = db.execute("SELECT * FROM sale_items WHERE sale_id = ?", (sale_id,)).fetchall()
    for item in items:
        db.execute(
            "UPDATE products SET stock_qty = stock_qty + ? WHERE id = ?",
            (item["quantity"], item["product_id"]),
        )
        db.execute(
            "INSERT INTO stock_movements (product_id, movement_type, quantity, note, created_at) VALUES (?, 'CANCEL', ?, ?, ?)",
            (item["product_id"], item["quantity"], f"Cancelamento venda #{sale_id}", now_iso()),
        )

    db.execute(
        "UPDATE sales SET status = 'CANCELLED', cancellation_reason = ? WHERE id = ?",
        (reason, sale_id),
    )
    db.commit()
    flash(f"Venda #{sale_id} cancelada e estoque retornado.", "success")
    return redirect(url_for("sales_history"))


@app.route("/sales")
def sales_history():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    sales = db.execute("SELECT * FROM sales ORDER BY id DESC LIMIT 100").fetchall()
    return render_template("sales_history.html", sales=sales, payment_methods=PAYMENT_METHODS)


@app.route("/sales/<int:sale_id>/receipt")
def sale_receipt(sale_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    sale = db.execute("SELECT * FROM sales WHERE id = ?", (sale_id,)).fetchone()
    items = db.execute("SELECT * FROM sale_items WHERE sale_id = ?", (sale_id,)).fetchall()
    return render_template("receipt.html", sale=sale, items=items)


@app.route("/cash", methods=["GET", "POST"])
def cash():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()

    if request.method == "POST":
        action = request.form["action"]

        if action == "open":
            opening_amount = float(request.form["opening_amount"])
            open_row = db.execute("SELECT id FROM cash_sessions WHERE status = 'OPEN'").fetchone()
            if open_row:
                flash("Já existe um caixa aberto.", "warning")
            else:
                db.execute(
                    "INSERT INTO cash_sessions (opened_at, opening_amount) VALUES (?, ?)",
                    (now_iso(), opening_amount),
                )
                db.commit()
                flash("Caixa aberto com sucesso.", "success")

        elif action == "close":
            reported = float(request.form["closing_amount_reported"])
            open_row = db.execute(
                "SELECT * FROM cash_sessions WHERE status = 'OPEN' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if not open_row:
                flash("Não há caixa aberto para fechar.", "danger")
            else:
                totals = db.execute(
                    """
                    SELECT
                      COALESCE(SUM(total),0) AS total_sales,
                      COALESCE(SUM(CASE WHEN payment_method = 'DINHEIRO' THEN total ELSE 0 END),0) AS cash_sales
                    FROM sales
                    WHERE status = 'ACTIVE' AND cash_session_id = ?
                    """,
                    (open_row["id"],),
                ).fetchone()
                expected_cash = open_row["opening_amount"] + totals["cash_sales"]
                diff = reported - expected_cash
                db.execute(
                    """
                    UPDATE cash_sessions
                    SET closed_at = ?, closing_amount_reported = ?, expected_cash = ?,
                        cash_difference = ?, total_sales = ?, status = 'CLOSED'
                    WHERE id = ?
                    """,
                    (now_iso(), reported, expected_cash, diff, totals["total_sales"], open_row["id"]),
                )
                db.commit()
                flash("Caixa fechado com sucesso.", "success")

        return redirect(url_for("cash"))

    open_session = db.execute(
        "SELECT * FROM cash_sessions WHERE status = 'OPEN' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    history = db.execute("SELECT * FROM cash_sessions ORDER BY id DESC LIMIT 30").fetchall()
    report = db.execute(
        """
        SELECT payment_method, COALESCE(SUM(total),0) AS total
        FROM sales
        WHERE status = 'ACTIVE' AND created_at >= ?
        GROUP BY payment_method
        """,
        (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),),
    ).fetchall()

    total_day = db.execute(
        "SELECT COALESCE(SUM(total),0) AS total FROM sales WHERE status = 'ACTIVE' AND created_at >= ?",
        (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),),
    ).fetchone()["total"]

    return render_template("cash.html", open_session=open_session, history=history, report=report, total_day=total_day)


@app.route("/reports")
def reports():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()

    top_products = db.execute(
        """
        SELECT product_name, SUM(quantity) AS qty, SUM(total_price) AS faturamento
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        WHERE s.status = 'ACTIVE'
        GROUP BY product_name
        ORDER BY qty DESC LIMIT 10
        """
    ).fetchall()

    now = datetime.now()
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

    def total_since(since: str) -> float:
        return db.execute(
            "SELECT COALESCE(SUM(total),0) AS t FROM sales WHERE status = 'ACTIVE' AND created_at >= ?",
            (since,),
        ).fetchone()["t"]

    faturamento = {
        "dia": total_since(day_start),
        "semana": total_since(week_start),
        "mes": total_since(month_start),
    }

    lucro = db.execute(
        """
        SELECT COALESCE(SUM((si.unit_price - p.cost_price) * si.quantity),0) AS lucro
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN products p ON p.id = si.product_id
        WHERE s.status = 'ACTIVE'
        """
    ).fetchone()["lucro"]

    stock_report = db.execute("SELECT * FROM products ORDER BY stock_qty ASC").fetchall()
    return render_template(
        "reports.html",
        top_products=top_products,
        faturamento=faturamento,
        lucro=lucro,
        stock_report=stock_report,
    )


@app.route("/monthly")
def monthly_management():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()

    months = db.execute(
        """
        SELECT substr(created_at, 1, 7) AS month_key, COUNT(*) AS total_vendas, COALESCE(SUM(total),0) AS faturamento
        FROM sales
        GROUP BY substr(created_at, 1, 7)
        ORDER BY month_key DESC
        """
    ).fetchall()

    month_status = {
        row["month_key"]: row
        for row in db.execute("SELECT * FROM monthly_closures").fetchall()
    }

    selected_month = request.args.get("month", "")
    show_month_details = request.args.get("show") == "1"
    month_sales = []
    if selected_month and show_month_details:
        month_sales = db.execute(
            "SELECT * FROM sales WHERE substr(created_at,1,7)=? ORDER BY id DESC",
            (selected_month,),
        ).fetchall()

    return render_template(
        "monthly.html",
        months=months,
        month_status=month_status,
        selected_month=selected_month,
        show_month_details=show_month_details,
        month_sales=month_sales,
        payment_methods=PAYMENT_METHODS,
    )


@app.post("/monthly/add-sale")
def monthly_add_sale():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    month_key = request.form["month_key"]
    if is_month_closed(db, month_key):
        flash(f"Mês {month_key} está fechado e não aceita novas vendas.", "danger")
        return redirect(url_for("monthly_management", month=month_key))
    created_at = f"{month_key}-01T12:00:00"
    total = float(request.form["total"])
    payment_method = request.form["payment_method"]
    note = request.form.get("note", "Ajuste manual mês")

    db.execute(
        "INSERT INTO sales (created_at, subtotal, discount, total, payment_method, status, cancellation_reason, cash_session_id) VALUES (?, ?, 0, ?, ?, 'ACTIVE', ?, NULL)",
        (created_at, total, total, payment_method, note),
    )
    db.commit()
    flash("Venda adicionada no mês selecionado.", "success")
    return redirect(url_for("monthly_management", month=month_key))


@app.post("/monthly/<month_key>/status")
def monthly_change_status(month_key: str):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    action = request.form["action"]
    new_status = "FECHADO" if action == "close" else "ATIVO"
    db.execute(
        "INSERT INTO monthly_closures (month_key, status, closed_at, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(month_key) DO UPDATE SET status=excluded.status, closed_at=excluded.closed_at",
        (month_key, new_status, now_iso() if new_status == "FECHADO" else None, now_iso()),
    )
    db.commit()
    flash(f"Mês {month_key} atualizado para {new_status}.", "success")
    return redirect(url_for("monthly_management", month=month_key))


@app.route("/mesa", methods=["GET", "POST"])
def mesa():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()

    if request.method == "POST":
        client_name = request.form["client_name"].strip()
        product_id = int(request.form["product_id"])
        quantity = int(request.form["quantity"])
        discount = parse_money(request.form.get("discount", "0"))
        note = request.form.get("note", "").strip()

        if not client_name:
            flash("Informe o nome do cliente.", "warning")
            return redirect(url_for("mesa"))
        if quantity <= 0:
            flash("Quantidade deve ser maior que zero.", "warning")
            return redirect(url_for("mesa"))

        product = db.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
        if not product:
            flash("Produto não encontrado.", "danger")
            return redirect(url_for("mesa"))
        if product["stock_qty"] <= 0:
            flash(f"Produto {product['name']} está zerado no estoque.", "danger")
            return redirect(url_for("mesa"))
        if product["stock_qty"] < quantity:
            flash(f"Estoque insuficiente para {product['name']}.", "danger")
            return redirect(url_for("mesa"))

        gross_total = quantity * product["sale_price"]
        total_debt = max(gross_total - discount, 0)

        db.execute(
            """
            INSERT INTO open_tabs (created_at, client_name, product_id, product_name, quantity, unit_price, discount, total_debt, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now_iso(), client_name, product_id, product["name"], quantity, product["sale_price"], discount, total_debt, note),
        )
        db.execute("UPDATE products SET stock_qty = stock_qty - ? WHERE id = ?", (quantity, product_id))
        db.execute(
            "INSERT INTO stock_movements (product_id, movement_type, quantity, note, created_at) VALUES (?, 'SALE', ?, ?, ?)",
            (product_id, -quantity, f"Fiado - {client_name}", now_iso()),
        )
        db.commit()
        flash("Cliente adicionado na lista de fiados e estoque atualizado.", "success")
        return redirect(url_for("mesa"))

    products = db.execute("SELECT * FROM products ORDER BY name").fetchall()
    open_tabs = db.execute("SELECT * FROM open_tabs ORDER BY id DESC LIMIT 200").fetchall()
    return render_template("mesa.html", products=products, open_tabs=open_tabs)


@app.post("/mesa/<int:tab_id>/delete")
def mesa_delete(tab_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    row = db.execute("SELECT id FROM open_tabs WHERE id = ?", (tab_id,)).fetchone()
    if not row:
        flash("Registro de fiado não encontrado.", "warning")
        return redirect(url_for("mesa"))
    db.execute("DELETE FROM open_tabs WHERE id = ?", (tab_id,))
    db.commit()
    flash("Cliente removido da lista de fiados.", "warning")
    return redirect(url_for("mesa"))


@app.post("/mesa/<int:tab_id>/update-note")
def mesa_update_note(tab_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    row = db.execute("SELECT * FROM open_tabs WHERE id = ?", (tab_id,)).fetchone()
    if not row:
        flash("Registro de fiado não encontrado.", "warning")
        return redirect(url_for("mesa"))

    note = request.form.get("note", "").strip()
    db.execute("UPDATE open_tabs SET note = ? WHERE id = ?", (note, tab_id))
    db.commit()
    flash("Observação atualizada.", "success")
    return redirect(url_for("mesa"))


@app.post("/mesa/<int:tab_id>/update-total")
def mesa_update_total(tab_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    row = db.execute("SELECT * FROM open_tabs WHERE id = ?", (tab_id,)).fetchone()
    if not row:
        flash("Registro de fiado não encontrado.", "warning")
        return redirect(url_for("mesa"))

    new_total = parse_money(request.form.get("total_debt", "0"))
    if new_total < 0:
        flash("Valor devido não pode ser negativo.", "danger")
        return redirect(url_for("mesa"))

    db.execute("UPDATE open_tabs SET total_debt = ? WHERE id = ?", (new_total, tab_id))
    db.commit()
    flash("Valor total devido atualizado.", "success")
    return redirect(url_for("mesa"))


@app.post("/monthly/sales/<int:sale_id>/delete")
def monthly_delete_sale(sale_id: int):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    sale = db.execute("SELECT * FROM sales WHERE id = ?", (sale_id,)).fetchone()
    if sale:
        month_key = sale["created_at"][:7]
        if is_month_closed(db, month_key):
            flash(f"Mês {month_key} está fechado e não permite remoção de venda.", "danger")
            return redirect(url_for("monthly_management", month=month_key))
        db.execute("DELETE FROM sale_items WHERE sale_id = ?", (sale_id,))
        db.execute("DELETE FROM sales WHERE id = ?", (sale_id,))
        db.commit()
        flash("Venda removida do mês.", "warning")
        return redirect(url_for("monthly_management", month=month_key))
    flash("Venda não encontrada.", "danger")
    return redirect(url_for("monthly_management"))


@app.get("/monthly/<month_key>/export")
def monthly_export(month_key: str):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    db = get_db()
    sales = db.execute(
        "SELECT id, created_at, total, payment_method, status FROM sales WHERE substr(created_at,1,7)=? ORDER BY created_at",
        (month_key,),
    ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Data", "Total", "Pagamento", "Status"])
    total = 0.0
    for s in sales:
        writer.writerow([s["id"], s["created_at"], f"{s['total']:.2f}", s["payment_method"], s["status"]])
        total += s["total"]
    writer.writerow([])
    writer.writerow(["Total do mês", "", f"{total:.2f}", "", ""])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=faturamento_{month_key}.csv"},
    )


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
