// Package api exposes the engine over HTTP. Handlers do request parsing
// and status-code mapping only; every rule about what's allowed to
// happen next lives in the engine package.
package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/stan-ley-tech/flowforge/engine/internal/engine"
	"github.com/stan-ley-tech/flowforge/engine/internal/model"
)

type Server struct {
	engine *engine.Engine
	mux    *http.ServeMux
}

func NewServer(e *engine.Engine) *Server {
	s := &Server{engine: e, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)

	s.mux.HandleFunc("POST /v1/workflows", s.registerWorkflow)
	s.mux.HandleFunc("GET /v1/workflows/{name}", s.getWorkflow)
	s.mux.HandleFunc("POST /v1/workflows/{name}/runs", s.startRun)

	s.mux.HandleFunc("GET /v1/runs", s.listRuns)
	s.mux.HandleFunc("GET /v1/runs/{id}", s.getRun)
	s.mux.HandleFunc("GET /v1/runs/{id}/history", s.getHistory)
	s.mux.HandleFunc("POST /v1/runs/{id}/cancel", s.cancelRun)

	s.mux.HandleFunc("POST /v1/tasks/poll", s.pollTask)
	s.mux.HandleFunc("POST /v1/tasks/{id}/heartbeat", s.heartbeat)
	s.mux.HandleFunc("POST /v1/tasks/{id}/complete", s.completeTask)
	s.mux.HandleFunc("POST /v1/tasks/{id}/fail", s.failTask)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) registerWorkflow(w http.ResponseWriter, r *http.Request) {
	var def model.WorkflowDef
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	saved, err := s.engine.RegisterWorkflow(r.Context(), def)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, saved)
}

func (s *Server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, errors.New("fetching a stored workflow definition is not yet exposed"))
}

func (s *Server) startRun(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req startRunRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	run, err := s.engine.StartRun(r.Context(), name, req.Input)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	workflow := r.URL.Query().Get("workflow")
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	runs, err := s.engine.ListRuns(r.Context(), workflow, limit)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.engine.GetRun(r.Context(), id)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	steps, err := s.engine.ListSteps(r.Context(), id)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runDetail{Run: run, Steps: steps})
}

func (s *Server) getHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	events, err := s.engine.ListHistory(r.Context(), id)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.engine.CancelRun(r.Context(), id); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelling"})
}

func (s *Server) pollTask(w http.ResponseWriter, r *http.Request) {
	var req pollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	task, err := s.engine.PollTask(r.Context(), req.Workflow, req.WorkerID)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	if task == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, taskToResponse(task))
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.engine.Heartbeat(r.Context(), id, req.LeaseToken); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "extended"})
}

func (s *Server) completeTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req completeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.engine.CompleteStep(r.Context(), id, req.LeaseToken, req.Result); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (s *Server) failTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req failRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.engine.FailStep(r.Context(), id, req.LeaseToken, req.Reason); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "failed"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}

func writeEngineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrWorkflowNotFound), errors.Is(err, engine.ErrRunNotFound), errors.Is(err, engine.ErrStepNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, engine.ErrLeaseMismatch), errors.Is(err, engine.ErrRunAlreadyTerminal):
		writeError(w, http.StatusConflict, err)
	default:
		log.Printf("api: internal error: %v", err)
		writeError(w, http.StatusInternalServerError, errors.New("internal error"))
	}
}
