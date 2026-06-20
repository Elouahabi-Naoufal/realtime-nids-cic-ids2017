import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime

st.set_page_config(page_title="IDS Real-time", page_icon="🛡️", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
* { font-family: 'Inter', sans-serif; }
.kpi {
    background: var(--background-color, transparent);
    border: 1px solid rgba(128,128,128,0.2);
    border-radius: 12px;
    padding: 22px 20px;
    text-align: center;
}
.kpi-label { font-size:.72rem; color:var(--text-color); opacity:.6; text-transform:uppercase; letter-spacing:.1em; font-weight:600; margin-bottom:6px; }
.kpi-val { font-size:2.4rem; font-weight:800; line-height:1; }
.kpi-sub { font-size:.75rem; opacity:.5; margin-top:4px; }
.live-badge { display:inline-flex; align-items:center; gap:6px; background:rgba(16,185,129,.1); border:1px solid rgba(16,185,129,.4); border-radius:20px; padding:4px 12px; font-size:.75rem; color:#10b981; font-weight:600; }
.live-dot { width:8px; height:8px; background:#10b981; border-radius:50%; animation:pulse 1.5s infinite; }
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.section-title { font-size:.8rem; opacity:.5; text-transform:uppercase; letter-spacing:.1em; font-weight:600; margin-bottom:12px; }
</style>
""", unsafe_allow_html=True)

ATTACK_LABELS = {
    "0":"BENIGN","1":"Bot","2":"DDoS","3":"DoS GoldenEye","4":"DoS Hulk",
    "5":"DoS Slowhttptest","6":"DoS Slowloris","7":"FTP-Patator","8":"Heartbleed",
    "9":"Infiltration","10":"PortScan","11":"SSH-Patator",
    "12":"Web Attack Brute Force","13":"Web Attack SQL Injection","14":"Web Attack XSS",
}
COLORS = ["#6366f1","#8b5cf6","#ec4899","#f97316","#eab308","#10b981","#06b6d4","#3b82f6","#ef4444","#84cc16","#f43f5e"]

# ── Sidebar (static, never flickers) ─────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Config")
    mongo_uri       = st.text_input("MongoDB URI", "mongodb://localhost:27017/")
    db_name         = st.text_input("Database", "ids_db")
    collection_name = st.text_input("Collection", "predictions")
    st.markdown("---")
    refresh_interval = st.slider("Refresh (s)", 1, 15, 3)
    if st.checkbox("🗑️ Clear database"):
        st.warning("Deletes all predictions.")
        if st.button("Confirm", use_container_width=True):
            MongoClient(mongo_uri)[db_name][collection_name].drop()
            st.rerun()

# ── Static header (never flickers) ───────────────────────────────────────────
h1, h2 = st.columns([3, 1])
with h1:
    st.markdown("## 🛡️ Network Intrusion Detection — Live Monitor")
    st.caption("CIC-IDS2017 · Random Forest + XGBoost + CNN-BiLSTM-Transformer")
with h2:
    st.markdown("<br>", unsafe_allow_html=True)
    status_placeholder = st.empty()

st.markdown("---")
main_area = st.empty()   # all dynamic content goes here

# Widgets must be OUTSIDE the fragment
f1, f2 = st.columns([2,1])
with f1:
    view = st.radio("Show", ["All","Attacks only","Normal only"], horizontal=True)
with f2:
    n_rows = st.select_slider("Rows", [25,50,100,200], value=50)

# ── Fragment: only this re-renders on refresh ─────────────────────────────────
@st.fragment(run_every=refresh_interval)
def refresh_data():
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        client.server_info()
        df = pd.DataFrame(list(client[db_name][collection_name].find()))
        connected = True
    except Exception:
        df = pd.DataFrame()
        connected = False

    if not connected:
        status_placeholder.error("MongoDB offline")
        main_area.error("Cannot connect to MongoDB.")
        return

    status_placeholder.markdown('<div class="live-badge"><div class="live-dot"></div>LIVE</div>', unsafe_allow_html=True)

    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    if df.empty:
        main_area.info("Waiting for data… Start Spark and the Kafka producer.")
        return

    total   = len(df)
    normal  = (df['status'] == 'NORMAL').sum()
    attacks = (df['status'] == 'ATTACK').sum()
    atk_pct = round(attacks / total * 100, 1) if total else 0
    xgb_types = df[df['status']=='ATTACK']['xgb_attack_type'].nunique() if 'xgb_attack_type' in df.columns else 0
    atk_df  = df[df['status'] == 'ATTACK']

    with main_area.container():
        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        for col, label, val, sub, color in [
            (k1, "Total Flows",  f"{total:,}",   "analyzed",           "#60a5fa"),
            (k2, "Normal",       f"{normal:,}",  "benign traffic",     "#10b981"),
            (k3, "Attacks",      f"{attacks:,}", f"{atk_pct}% of total","#ef4444"),
            (k4, "Attack Types", xgb_types,      "unique (XGBoost)",   "#a78bfa"),
        ]:
            col.markdown(f'<div class="kpi"><div class="kpi-label">{label}</div><div class="kpi-val" style="color:{color}">{val}</div><div class="kpi-sub">{sub}</div></div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Row 1: Donut + Timeline
        c1, c2 = st.columns([1, 2])
        with c1:
            st.markdown('<div class="section-title">Traffic Distribution</div>', unsafe_allow_html=True)
            fig = go.Figure(go.Pie(
                labels=["Normal","Attack"], values=[normal, attacks], hole=0.65,
                marker_colors=["#10b981","#ef4444"], textinfo="percent",
                hovertemplate="%{label}: %{value:,}<extra></extra>",
            ))
            fig.add_annotation(text=f"<b>{atk_pct}%</b><br>attacks", x=0.5, y=0.5,
                               showarrow=False, font=dict(size=16, color="#e2e8f0"))
            fig.update_layout(showlegend=True, margin=dict(t=10,b=10,l=10,r=10),
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", y=-0.05, x=0.5, xanchor="center", font=dict(color="gray")))
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown('<div class="section-title">Flow Activity Over Time</div>', unsafe_allow_html=True)
            if 'timestamp' in df.columns and not df['timestamp'].isna().all():
                tl = df.copy()
                tl['minute'] = tl['timestamp'].dt.floor('1min')
                tl = tl.groupby(['minute','status']).size().reset_index(name='count')
                fig2 = px.area(tl, x='minute', y='count', color='status',
                               color_discrete_map={'NORMAL':'#10b981','ATTACK':'#ef4444'},
                               template="plotly")
                fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    margin=dict(t=10,b=10,l=10,r=10),
                    xaxis=dict(showgrid=False, color="gray"),
                    yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.2)", color="gray"),
                    legend=dict(orientation="h", y=-0.15, font=dict(color="gray")))
                st.plotly_chart(fig2, use_container_width=True)

        # Row 2: XGBoost + DL
        r2c1, r2c2 = st.columns(2)
        with r2c1:
            st.markdown('<div class="section-title">Attack Types — XGBoost</div>', unsafe_allow_html=True)
            if 'xgb_attack_type' in atk_df.columns and not atk_df.empty:
                xc = atk_df['xgb_attack_type'].map(lambda x: ATTACK_LABELS.get(str(x), str(x))).value_counts().reset_index()
                xc.columns = ["Type","Count"]
                fig3 = px.bar(xc.sort_values("Count"), y="Type", x="Count", orientation='h',
                              color="Type", color_discrete_sequence=COLORS, template="plotly")
                fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False, margin=dict(t=10,b=10,l=10,r=10),
                    xaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.2)", color="gray"),
                    yaxis=dict(showgrid=False, color="gray"))
                st.plotly_chart(fig3, use_container_width=True)
            else:
                st.info("No attacks detected yet.")

        with r2c2:
            st.markdown('<div class="section-title">Attack Types — Deep Learning</div>', unsafe_allow_html=True)
            if 'dl_prediction' in atk_df.columns and not atk_df.empty:
                dc = atk_df['dl_prediction'].value_counts().reset_index()
                dc.columns = ["Type","Count"]
                fig4 = px.bar(dc.sort_values("Count"), y="Type", x="Count", orientation='h',
                              color="Type", color_discrete_sequence=COLORS[::-1], template="plotly")
                fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False, margin=dict(t=10,b=10,l=10,r=10),
                    xaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.2)", color="gray"),
                    yaxis=dict(showgrid=False, color="gray"))
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info("No DL predictions yet.")

        # Live feed
        st.markdown("---")
        st.markdown('<div class="section-title">Live Predictions Feed</div>', unsafe_allow_html=True)

        table = df.sort_values('timestamp', ascending=False) if 'timestamp' in df.columns else df
        if view == "Attacks only":   table = table[table['status']=='ATTACK']
        elif view == "Normal only":  table = table[table['status']=='NORMAL']

        disp = table.head(n_rows).copy()
        if 'true_label'    in disp.columns: disp['true_label']    = disp['true_label'].map(lambda x: ATTACK_LABELS.get(str(x), str(x)))
        if 'rf_prediction' in disp.columns: disp['rf_prediction'] = disp['rf_prediction'].map(lambda x: ATTACK_LABELS.get(str(x), str(x)))
        if 'xgb_attack_type' in disp.columns: disp['xgb_attack_type'] = disp['xgb_attack_type'].map(lambda x: ATTACK_LABELS.get(str(x), str(x)) if str(x) not in ("None","nan") else "—")
        if 'timestamp'     in disp.columns: disp['timestamp']     = disp['timestamp'].dt.strftime('%H:%M:%S')

        cols = [c for c in ['timestamp','flow_id','status','true_label','rf_prediction','xgb_attack_type','dl_prediction','model_used'] if c in disp.columns]
        disp = disp[cols]

        def color_status(val):
            if val == 'ATTACK': return 'color:#ef4444;font-weight:700'
            if val == 'NORMAL': return 'color:#10b981;font-weight:600'
            return ''

        st.dataframe(
            disp.style.map(color_status, subset=['status']) if 'status' in disp.columns else disp,
            column_config={
                "timestamp": st.column_config.TextColumn("Time"),
                "flow_id": st.column_config.NumberColumn("Flow ID"),
                "status": st.column_config.TextColumn("Status"),
                "true_label": st.column_config.TextColumn("True Label"),
                "rf_prediction": st.column_config.TextColumn("Random Forest"),
                "xgb_attack_type": st.column_config.TextColumn("XGBoost"),
                "dl_prediction": st.column_config.TextColumn("Deep Learning"),
                "model_used": st.column_config.TextColumn("Models"),
            },
            use_container_width=True, hide_index=True, height=400,
        )

        st.markdown(f"<p style='text-align:center;opacity:.4;font-size:.75rem;margin-top:10px'>Last updated: {datetime.now().strftime('%H:%M:%S')} · refreshing every {refresh_interval}s</p>", unsafe_allow_html=True)

refresh_data()
