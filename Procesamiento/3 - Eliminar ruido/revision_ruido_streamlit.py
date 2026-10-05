"""Arranque: python -m streamlit run <este archivo> -- [--resume]."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import sys

# También permite ejecutar la interfaz con AppTest o desde otra carpeta.
STEP3_DIR = Path(__file__).resolve().parent
if str(STEP3_DIR) not in sys.path:
    sys.path.insert(0, str(STEP3_DIR))

from revision_ruido_core import DEFAULT_CSV, DELETE_RECORD, ReviewStore, apply_command, prepare_store


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Revisión humana persistente de los candidatos del paso 3.")
    parser.add_argument("--review-csv", type=Path, default=DEFAULT_CSV, help="CSV completo de auditoría, no la muestra.")
    parser.add_argument("--state-file", type=Path, help="Por defecto revision_ruido.sqlite3 junto al CSV.")
    parser.add_argument("--export-csv", type=Path, help="Por defecto candidatos_ruido_revisados.csv junto al CSV.")
    parser.add_argument("--resume", action="store_true", help="Retoma la importación, decisiones y posición guardadas.")
    parser.add_argument("--session-goal", type=int, default=20, help="Meta orientativa de candidatos por sesión.")
    parser.add_argument("--preview-chars", type=int, default=12000, help="Longitud inicial de la vista previa del fragmento.")
    args = parser.parse_args(argv)
    if args.session_goal < 1 or args.preview_chars < 1:
        parser.error("--session-goal y --preview-chars deben ser positivos.")
    args.review_csv = args.review_csv.resolve()
    args.state_file = (args.state_file or args.review_csv.with_name("revision_ruido.sqlite3")).resolve()
    args.export_csv = (args.export_csv or args.review_csv.with_name("candidatos_ruido_revisados.csv")).resolve()
    return args


def main(argv=None):
    args = parse_args(argv)
    import streamlit as st

    st.set_page_config(page_title="Revisión de ruido · Paso 3", layout="wide")
    st.title("Revisión de candidatos del paso 3")
    st.caption("Revisá de a poco. Solo se guardan los cambios al pulsar un botón de guardar; esta pantalla no elimina texto del corpus.")

    @st.cache_resource(show_spinner=False)
    def initialize(source_path: str, state_path: str, resume: bool):
        label = st.empty()
        bar = st.progress(0.0)

        def report(current, total, phase):
            label.text(f"{phase}: {current:,}/{total:,}")
            bar.progress(min(current / total, 1.0) if total else 1.0)

        prepare_store(Path(source_path), Path(state_path), resume=resume, progress=report)
        label.empty()
        bar.empty()
        return state_path

    try:
        initialize(str(args.review_csv), str(args.state_file), args.resume)
        store = ReviewStore(args.state_file)
        metadata = store.metadata()
    except (OSError, ValueError, sqlite3.Error) as error:
        st.error(str(error))
        st.info("La primera vez arrancá sin --resume. En las siguientes, agregá --resume después de los dos guiones -- de Streamlit.")
        st.stop()

    counts = metadata["counts"]
    total, pending = counts["total"], counts["pending"]
    reviewed = total - pending
    for column, name, value in zip(st.columns(5), ("Total", "Revisados", "Pendientes", "Eliminar", "Conservar"),
                                   (total, reviewed, pending, counts["eliminar"], counts["conservar"])):
        column.metric(name, f"{value:,}")
    st.progress(reviewed / total if total else 1.0)
    st.caption(f"Avance global: {reviewed:,}/{total:,} ({100 * reviewed / total if total else 100:.2f}%). Pendiente no cuenta como revisado.")

    if "session_reviews" not in st.session_state:
        st.session_state.session_reviews = set()
    session_count = len(st.session_state.session_reviews)
    st.sidebar.header("Esta sesión")
    goal = st.sidebar.number_input("Meta de candidatos por sesión", min_value=1, value=args.session_goal, step=1, key="session_goal")
    st.sidebar.metric("Resueltos en esta sesión", session_count)
    st.sidebar.progress(min(session_count / goal, 1.0))
    st.sidebar.caption(f"Faltan {max(goal - session_count, 0)} para tu meta. Podés parar en cualquier momento.")
    if session_count >= goal:
        st.sidebar.success("Llegaste a tu meta. Podés cerrar y retomar otro día.")

    st.sidebar.header("Filtros")
    status_label = st.sidebar.selectbox("Decisión", ("Pendientes", "Todos", "Eliminar", "Conservar"), key="status_filter")
    origin = st.sidebar.selectbox("Base de origen", [None, *metadata["origins"]],
                                  format_func=lambda value: "Todas las bases" if value is None else value, key="origin_filter")
    reason = st.sidebar.selectbox("Motivo", [None, *metadata["reasons"]],
                                  format_func=lambda value: "Todos los motivos" if value is None else value, key="reason_filter")
    status = {"Pendientes": "", "Todos": None, "Eliminar": "eliminar", "Conservar": "conservar"}[status_label]
    filters = {"status": status, "origin": origin, "reason": reason}
    signature = (status, origin, reason)
    filtered_count = store.count(**filters)
    st.sidebar.caption(f"Candidatos en estos filtros: {filtered_count:,}")
    if st.session_state.get("filter_signature") != signature:
        st.session_state.pop("stay_seq", None)
        st.session_state.filter_signature = signature
        # Al arrancar, conserva el cursor si cumple los filtros; al cambiar filtros,
        # vuelve al primer candidato de la nueva selección.
        if "current_seq" not in st.session_state:
            bookmark = int(metadata["last_seq"])
            st.session_state.current_seq = store.find(after=bookmark - 1, **filters)
        else:
            st.session_state.current_seq = store.find(**filters)
    current_seq = st.session_state.get("current_seq")
    candidate = store.candidate(current_seq) if current_seq else None
    if candidate and st.session_state.get("stay_seq") != candidate["seq"] and ((status is not None and candidate["decision"] != status)
                      or (origin is not None and candidate["base_de_datos_origen"] != origin)
                      or (reason is not None and reason not in candidate["codigos_motivo"].split("|"))):
        current_seq = store.find(after=int(candidate["seq"]), **filters)
        st.session_state.current_seq = current_seq
        candidate = store.candidate(current_seq) if current_seq else None

    if message := st.session_state.pop("save_message", None):
        st.success(message)

    st.sidebar.header("Exportación para apply")
    dirty = int(metadata["export_revision"]) != int(metadata["revision"])
    if metadata["export_path"] and not dirty:
        st.sidebar.success("El CSV exportado está actualizado.")
    elif metadata["export_path"]:
        st.sidebar.warning("Hay decisiones nuevas: volvé a exportar antes de apply.")
    else:
        st.sidebar.caption("Todavía no exportaste esta revisión.")
    st.sidebar.caption("Se exportan TODOS los candidatos, no solo los del filtro. Los pendientes se conservan al aplicar.")
    if st.sidebar.button("Exportar CSV completo para apply", key="export"):
        try:
            with st.spinner("Verificando y exportando todas las decisiones..."):
                result = store.export(args.export_csv)
            st.session_state.save_message = f"CSV completo exportado: {result['path']}. Podés ejecutar apply; los pendientes se conservan."
            st.rerun()
        except (OSError, ValueError, sqlite3.Error) as error:
            st.error(str(error))

    if metadata["export_path"]:
        with st.expander("Comandos para aplicar el CSV exportado", expanded=not dirty):
            if dirty:
                st.warning("Estos comandos usan la última exportación, que no incluye los cambios posteriores. Primero exportá de nuevo.")
            input_path = Path(str(metadata["input_path"]))
            st.code(apply_command(input_path, Path(str(metadata["export_path"]))), language="powershell")
            st.caption("Solo si se interrumpe apply, retomalo con:")
            st.code(apply_command(input_path, Path(str(metadata["export_path"])), resume=True), language="powershell")
            st.caption("Si cambiás decisiones después de iniciar apply, necesitás exportar de nuevo e iniciar una NUEVA aplicación; no sirve --resume con un CSV cambiado.")

    if candidate is None:
        if pending == 0:
            st.success("Terminaste la revisión: no quedan candidatos pendientes. Exportá el CSV completo para aplicar las decisiones.")
        else:
            st.info("No hay candidatos para estos filtros. Cambiá los filtros para seguir revisando.")
        return

    seq = int(candidate["seq"])
    st.subheader(f"Candidato {seq:,} de {total:,}")
    st.caption("El número indica su posición en el CSV original, no cuántos ya revisaste.")
    st.text(f"Motivo: {candidate['tipo_deteccion']}\nCódigos: {candidate['codigos_motivo']}")
    st.text(f"Acción propuesta: {candidate['accion_propuesta']} · Nivel: {candidate['nivel']} · Confianza heurística: {candidate['confianza']}")
    st.caption("La confianza es una señal del detector, no una aprobación automática.")
    st.text(f"Origen: {candidate['base_de_datos_origen']} · ID: {candidate['id']} · Registro: {candidate['numero_registro']}\n"
            f"Archivo: {candidate['archivo_origen']}\nRuta: {candidate['ruta_relativa_origen']}")
    full_record = candidate["accion_propuesta"] == DELETE_RECORD
    if full_record:
        st.warning("Esta propuesta elimina EL REGISTRO COMPLETO, no solo una parte de su texto.")

    context_column, metrics_column = st.columns([3, 2])
    with context_column:
        st.markdown("#### Contexto anterior")
        st.code(candidate["contexto_antes"] or "(Sin contexto anterior)", language=None, wrap_lines=True)
        st.markdown("#### Fragmento propuesto para eliminar")
        fragment = str(candidate["texto_detectado"])
        show_full = False
        if len(fragment) > args.preview_chars:
            st.warning(f"Fragmento largo: {len(fragment):,} caracteres. La decisión se aplica a TODO el fragmento, no solo a la vista previa.")
            show_full = st.checkbox("Mostrar fragmento completo (puede ser largo)", key=f"full_{seq}")
        st.code(fragment if show_full else fragment[:args.preview_chars], language=None, wrap_lines=True)
        st.caption(f"Caracteres {candidate['inicio_caracter']} a {candidate['fin_caracter']} (fin exclusivo). Longitud: {len(fragment):,}.")
        st.markdown("#### Contexto posterior")
        st.code(candidate["contexto_despues"] or "(Sin contexto posterior)", language=None, wrap_lines=True)
    with metrics_column:
        st.markdown("#### Métricas del detector")
        st.json(json.loads(candidate["metricas"]), expanded=True)
        with st.expander("Trazabilidad"):
            st.json({key: candidate[key] for key in ("candidate_id", "version_auditoria", "hash_registro", "hash_fragmento")})

    labels = {"": "Pendiente", "eliminar": "Eliminar", "conservar": "Conservar"}
    form_key = f"review_{seq}_{candidate['revision']}"
    with st.form(form_key, enter_to_submit=False):
        decision_label = st.radio("Tu decisión", tuple(labels.values()),
                                  index=list(labels).index(candidate["decision"]), horizontal=True, key="decision_" + form_key)
        notes = st.text_area("Comentarios / notas de revisión", value=candidate["notas_revision"], key="notes_" + form_key)
        confirm_record = st.checkbox("Confirmo que quiero eliminar el registro completo", key="confirm_" + form_key) if full_record else True
        save_col, stay_col, skip_col = st.columns(3)
        save_next = save_col.form_submit_button("Guardar y seguir", type="primary")
        save_stay = stay_col.form_submit_button("Guardar sin avanzar")
        mark_pending = skip_col.form_submit_button("Marcar pendiente y seguir")
    if save_next or save_stay or mark_pending:
        decision = "" if mark_pending else {value: key for key, value in labels.items()}[decision_label]
        if full_record and decision == "eliminar" and not confirm_record:
            st.error("Para aprobar esta propuesta, confirmá explícitamente la eliminación del registro completo.")
        else:
            try:
                store.save(seq, decision, notes, expected_revision=int(candidate["revision"]))
                if not candidate["decision"] and decision:
                    st.session_state.session_reviews.add(seq)
                elif not decision:
                    st.session_state.session_reviews.discard(seq)
                if save_next or mark_pending:
                    st.session_state.pop("stay_seq", None)
                    next_seq = store.find(after=seq, **filters)
                    st.session_state.current_seq = next_seq
                    store.remember(next_seq)
                else:
                    st.session_state.stay_seq = seq
                st.session_state.save_message = "Decisión y comentarios guardados. Podés cerrar sin perderlos."
                st.rerun()
            except (OSError, ValueError, sqlite3.Error) as error:
                st.error(str(error))

    previous_col, next_col = st.columns(2)
    if previous_col.button("Anterior", key="previous"):
        st.session_state.pop("stay_seq", None)
        next_seq = store.find(after=seq, direction=-1, **filters)
        st.session_state.current_seq = next_seq
        store.remember(next_seq)
        st.rerun()
    if next_col.button("Siguiente sin guardar", key="next"):
        st.session_state.pop("stay_seq", None)
        next_seq = store.find(after=seq, **filters)
        st.session_state.current_seq = next_seq
        store.remember(next_seq)
        st.rerun()
    st.caption("Anterior / Siguiente sin guardar no guardan lo que escribiste. Para pausar, pulsá Guardar sin avanzar y después cerrá la pestaña o detené el servidor con Ctrl+C.")


if __name__ == "__main__":
    main()
