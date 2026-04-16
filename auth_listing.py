"""
auth_listing.py — авторизація для Amazon Listing Analyzer
Таблиці: listing_users, listing_user_asins
"""

import os
import bcrypt
import psycopg2
import streamlit as st
from datetime import datetime

# ─── DB ───────────────────────────────────────────────────────────────────────

def _get_conn():
    db_url = st.secrets.get("DATABASE_URL", "")
    if not db_url:
        return None
    try:
        import psycopg2
        return psycopg2.connect(db_url, sslmode="require")
    except Exception as e:
        return None


def auth_db_init():
    """Створює таблиці якщо не існують + колонка analyzed_by у listing_analysis."""
    conn = _get_conn()
    if not conn: return
    cur = conn.cursor()

    # Таблиця юзерів
    cur.execute("""
        CREATE TABLE IF NOT EXISTS listing_users (
            id         SERIAL PRIMARY KEY,
            email      TEXT UNIQUE NOT NULL,
            password   TEXT NOT NULL,
            name       TEXT,
            role       TEXT DEFAULT 'viewer',   -- admin | viewer
            is_active  BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            last_login TIMESTAMP
        )
    """)

    # Права — які ASINи бачить viewer
    # NULL asin_filter = бачить усі (для admin або якщо права не обмежені)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS listing_user_asins (
            user_id  INTEGER REFERENCES listing_users(id) ON DELETE CASCADE,
            asin     TEXT NOT NULL,
            PRIMARY KEY (user_id, asin)
        )
    """)

    # Додаємо analyzed_by у listing_analysis якщо нема
    try:
        cur.execute("""
            ALTER TABLE listing_analysis
            ADD COLUMN IF NOT EXISTS analyzed_by TEXT DEFAULT ''
        """)
    except Exception:
        pass

    conn.commit()

    # Дефолтний адмін якщо таблиця порожня
    cur.execute("SELECT COUNT(*) FROM listing_users")
    if cur.fetchone()[0] == 0:
        admin_email = st.secrets.get("ADMIN_EMAIL", "admin@merino.tech")
        admin_pass  = st.secrets.get("ADMIN_PASSWORD", "admin123")
        hashed = bcrypt.hashpw(admin_pass.encode(), bcrypt.gensalt()).decode()
        cur.execute("""
            INSERT INTO listing_users (email, password, name, role, is_active)
            VALUES (%s, %s, 'Administrator', 'admin', TRUE)
        """, (admin_email, hashed))
        conn.commit()

    cur.close(); conn.close()


# ─── AUTH FUNCTIONS ───────────────────────────────────────────────────────────

def auth_verify_login(email: str, password: str):
    """Повертає dict юзера або None."""
    try:
        conn = _get_conn()
        if not conn: return None
        cur = conn.cursor()
        cur.execute("""
            SELECT id, email, password, name, role, is_active
            FROM listing_users WHERE email = %s
        """, (email.strip().lower(),))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row: return None
        uid, em, pwd_hash, name, role, is_active = row
        if not is_active: return None
        if bcrypt.checkpw(password.encode(), pwd_hash.encode()):
            conn2 = _get_conn(); cur2 = conn2.cursor()
            cur2.execute("UPDATE listing_users SET last_login = NOW() WHERE id = %s", (uid,))
            conn2.commit(); cur2.close(); conn2.close()
            return {"id": uid, "email": em, "name": name or em, "role": role}
        return None
    except Exception as e:
        st.error(f"Auth error: {e}")
        return None


def auth_get_allowed_asins(user_id: int):
    """Повертає set ASINів або None (= всі)."""
    try:
        conn = _get_conn()
        if not conn: return None
        cur = conn.cursor()
        cur.execute("""
            SELECT asin FROM listing_user_asins WHERE user_id = %s
        """, (user_id,))
        rows = cur.fetchall(); cur.close(); conn.close()
        return {r[0] for r in rows} if rows else None  # None = бачить всі
    except:
        return None


def auth_can_see_asin(asin: str) -> bool:
    """Перевіряє чи поточний юзер може бачити цей ASIN."""
    user = st.session_state.get("_auth_user")
    if not user: return False
    if user["role"] == "admin": return True
    allowed = st.session_state.get("_auth_asins")  # None = всі
    if allowed is None: return True
    return asin in allowed


def auth_current_user_name() -> str:
    user = st.session_state.get("_auth_user")
    return user["name"] if user else ""


def auth_current_user_email() -> str:
    user = st.session_state.get("_auth_user")
    return user["email"] if user else ""


def auth_is_admin() -> bool:
    user = st.session_state.get("_auth_user")
    return user["role"] == "admin" if user else False


def auth_logout():
    for k in ["_auth_user", "_auth_asins"]:
        st.session_state.pop(k, None)
    st.rerun()


# ─── LOGIN FORM ───────────────────────────────────────────────────────────────

def auth_show_login():
    """Показує форму логіну. Повертає True якщо залогінений."""
    user = st.session_state.get("_auth_user")
    if user:
        return True

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="text-align:center;padding:32px 0 20px">
            <img src="https://merino.tech/cdn/shop/files/MT_logo_1.png?v=1685099753&width=260"
                 style="max-width:200px">
            <div style="font-size:13px;color:#64748b;margin-top:8px;font-weight:600">
                Listing Analyzer — Internal Tool
            </div>
        </div>
        """, unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown("#### 🔐 Вхід")
            email    = st.text_input("📧 Email", placeholder="your@email.com", key="la_email")
            password = st.text_input("🔑 Пароль", type="password", key="la_password")

            if st.button("Увійти →", type="primary", use_container_width=True, key="la_login_btn"):
                if not email or not password:
                    st.error("Введіть email і пароль")
                else:
                    u = auth_verify_login(email, password)
                    if u:
                        st.session_state["_auth_user"] = u
                        # Завантажити дозволені ASINи
                        if u["role"] == "admin":
                            st.session_state["_auth_asins"] = None  # всі
                        else:
                            st.session_state["_auth_asins"] = auth_get_allowed_asins(u["id"])
                        st.rerun()
                    else:
                        st.error("❌ Невірний email або пароль / акаунт не активовано")
    return False


# ─── SIDEBAR USER BADGE ───────────────────────────────────────────────────────

def auth_show_sidebar_user():
    """Показує бейдж юзера в сайдбарі + кнопку logout."""
    user = st.session_state.get("_auth_user")
    if not user: return
    role_icon  = "👑" if user["role"] == "admin" else "👤"
    role_label = "Admin" if user["role"] == "admin" else "Viewer"
    st.sidebar.markdown(
        f'<div style="background:#1e293b;border-radius:8px;padding:8px 12px;margin-bottom:8px">'
        f'<div style="font-size:0.78rem;font-weight:700;color:#e2e8f0">{role_icon} {user["name"]}</div>'
        f'<div style="font-size:0.68rem;color:#64748b">{user["email"]} · {role_label}</div>'
        f'</div>',
        unsafe_allow_html=True
    )
    if st.sidebar.button("🚪 Вийти", key="la_logout", use_container_width=True):
        auth_logout()


# ─── HISTORY BADGE ────────────────────────────────────────────────────────────

def auth_user_badge_html(analyzed_by: str) -> str:
    """HTML бейдж 'хто аналізував' для карток історії."""
    if not analyzed_by:
        return ""
    # Скорочуємо до імені або першої частини email
    short = analyzed_by.split("@")[0] if "@" in analyzed_by else analyzed_by
    short = short[:12]
    return (
        f' &nbsp;·&nbsp; <span style="background:#312e81;color:#a5b4fc;'
        f'border-radius:4px;padding:1px 6px;font-size:0.68rem;font-weight:600">'
        f'👤 {short}</span>'
    )


# ─── ADMIN PANEL ─────────────────────────────────────────────────────────────

def auth_show_admin_panel():
    """Панель управління юзерами для адміна."""
    st.markdown("## 👑 Управління користувачами")

    tab_users, tab_create = st.tabs(["👥 Користувачі", "➕ Додати"])

    with tab_users:
        conn = _get_conn()
        if not conn: st.error("Немає підключення до БД"); return
        cur = conn.cursor()
        cur.execute("""
            SELECT id, email, name, role, is_active, created_at, last_login
            FROM listing_users ORDER BY created_at DESC
        """)
        users = cur.fetchall(); cur.close(); conn.close()

        if not users:
            st.info("Юзерів немає"); return

        current_id = st.session_state["_auth_user"]["id"]

        for uid, email, name, role, is_active, created_at, last_login in users:
            is_self  = uid == current_id
            is_admin_u = role == "admin"
            last_str = last_login.strftime("%d.%m.%Y %H:%M") if last_login else "ніколи"

            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 2, 2, 2])
                c1.markdown(f"**{name or '—'}**{'  *(ви)*' if is_self else ''}")
                c1.caption(email)
                c2.markdown(f"`{role.upper()}`")
                c3.markdown("🟢 Активний" if is_active else "🔴 Вимкнений")
                c4.caption(f"🕐 {last_str}")

                if not is_self:
                    with st.expander(f"⚙️ {name or email}"):
                        col1, col2, col3 = st.columns(3)

                        # Роль
                        with col1:
                            new_role = st.selectbox("Роль:", ["admin","viewer"],
                                index=0 if role=="admin" else 1, key=f"la_role_{uid}")
                            if st.button("💾 Роль", key=f"la_save_role_{uid}", use_container_width=True):
                                conn2 = _get_conn(); cur2 = conn2.cursor()
                                cur2.execute("UPDATE listing_users SET role=%s WHERE id=%s", (new_role, uid))
                                conn2.commit(); cur2.close(); conn2.close()
                                st.success("✅"); st.rerun()

                        # Статус
                        with col2:
                            btn_lbl = "🚫 Деактивувати" if is_active else "✅ Активувати"
                            if st.button(btn_lbl, key=f"la_act_{uid}", use_container_width=True):
                                conn2 = _get_conn(); cur2 = conn2.cursor()
                                cur2.execute("UPDATE listing_users SET is_active=%s WHERE id=%s",
                                             (not is_active, uid))
                                conn2.commit(); cur2.close(); conn2.close()
                                st.rerun()

                        # Видалити
                        with col3:
                            if st.button("🗑 Видалити", key=f"la_del_{uid}", use_container_width=True):
                                conn2 = _get_conn(); cur2 = conn2.cursor()
                                cur2.execute("DELETE FROM listing_users WHERE id=%s", (uid,))
                                conn2.commit(); cur2.close(); conn2.close()
                                st.rerun()

                        # Новий пароль
                        st.markdown("---")
                        new_pw = st.text_input("🔑 Новий пароль:", type="password", key=f"la_pw_{uid}")
                        if st.button("💾 Змінити пароль", key=f"la_save_pw_{uid}", use_container_width=True):
                            if new_pw and len(new_pw) >= 6:
                                hashed = bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt()).decode()
                                conn2 = _get_conn(); cur2 = conn2.cursor()
                                cur2.execute("UPDATE listing_users SET password=%s WHERE id=%s", (hashed, uid))
                                conn2.commit(); cur2.close(); conn2.close()
                                st.success("✅ Пароль змінено!")
                            else:
                                st.error("Мін. 6 символів")

                        # ASINи доступу (тільки для viewer)
                        if role != "admin":
                            st.markdown("---")
                            st.markdown("**📦 Доступні ASINи** (порожньо = всі)")
                            allowed = auth_get_allowed_asins(uid) or set()
                            asins_str = st.text_area(
                                "ASINи через кому або новий рядок:",
                                value="\n".join(sorted(allowed)),
                                height=100, key=f"la_asins_{uid}"
                            )
                            if st.button("💾 Зберегти ASINи", key=f"la_save_asins_{uid}",
                                         use_container_width=True, type="primary"):
                                import re as _re
                                new_asins = [a.strip().upper() for a in _re.split(r"[,\n\s]+", asins_str) if a.strip()]
                                conn2 = _get_conn(); cur2 = conn2.cursor()
                                cur2.execute("DELETE FROM listing_user_asins WHERE user_id=%s", (uid,))
                                for a in new_asins:
                                    cur2.execute("""
                                        INSERT INTO listing_user_asins (user_id, asin)
                                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                                    """, (uid, a))
                                conn2.commit(); cur2.close(); conn2.close()
                                st.success(f"✅ {len(new_asins)} ASINів збережено")

    with tab_create:
        st.markdown("### ➕ Новий користувач")
        with st.container(border=True):
            col1, col2 = st.columns(2)
            with col1:
                nc_email = st.text_input("📧 Email:", key="la_nc_email")
                nc_name  = st.text_input("👤 Ім'я:", key="la_nc_name")
            with col2:
                nc_pass  = st.text_input("🔑 Пароль:", type="password", key="la_nc_pass")
                nc_role  = st.selectbox("Роль:", ["viewer", "admin"], key="la_nc_role")

            nc_asins = ""
            if nc_role == "viewer":
                st.markdown("**📦 ASINи доступу** (порожньо = всі):")
                nc_asins = st.text_area("ASINи через кому:", height=80, key="la_nc_asins")

            if st.button("✅ Створити", type="primary", use_container_width=True, key="la_nc_create"):
                if not nc_email or not nc_pass:
                    st.error("Email і пароль обов'язкові")
                elif len(nc_pass) < 6:
                    st.error("Пароль мін. 6 символів")
                else:
                    try:
                        hashed = bcrypt.hashpw(nc_pass.encode(), bcrypt.gensalt()).decode()
                        conn2 = _get_conn(); cur2 = conn2.cursor()
                        cur2.execute("""
                            INSERT INTO listing_users (email, password, name, role, is_active)
                            VALUES (%s, %s, %s, %s, TRUE) RETURNING id
                        """, (nc_email.strip().lower(), hashed, nc_name, nc_role))
                        new_id = cur2.fetchone()[0]
                        # ASINи якщо viewer
                        if nc_role == "viewer" and nc_asins.strip():
                            import re as _re
                            for a in _re.split(r"[,\n\s]+", nc_asins):
                                a = a.strip().upper()
                                if a:
                                    cur2.execute("""
                                        INSERT INTO listing_user_asins (user_id, asin)
                                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                                    """, (new_id, a))
                        conn2.commit(); cur2.close(); conn2.close()
                        st.success(f"✅ Юзер {nc_email} створений!")
                        st.rerun()
                    except Exception as e:
                        err = str(e)
                        if "unique" in err.lower():
                            st.error("Email вже існує")
                        else:
                            st.error(f"Помилка: {err}")
