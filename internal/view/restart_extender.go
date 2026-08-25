// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of K9s

package view

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/derailed/k9s/internal/client"
	"github.com/derailed/k9s/internal/dao"
	"github.com/derailed/k9s/internal/port"
	"github.com/derailed/k9s/internal/ui"
	"github.com/derailed/k9s/internal/ui/dialog"
	"github.com/derailed/tcell/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// pfRestorePollInterval is how often we check for a fresh, ready replacement pod.
	pfRestorePollInterval = 2 * time.Second
	// pfRestoreTimeout is how long we wait for a fresh, ready replacement pod.
	pfRestoreTimeout = 3 * time.Minute
	// pfRestoreSettleDelay gives the new pod's containers a moment to start listening
	// after the Ready condition flips, since readiness can race the app's own startup.
	pfRestoreSettleDelay = 3 * time.Second
	// pfRestoreMaxAttempts is how many times we retry a forward that fails immediately.
	pfRestoreMaxAttempts = 5
	// pfRestoreRetryDelay is the backoff between failed forward attempts.
	pfRestoreRetryDelay = 2 * time.Second
	// pfRestoreGraceWindow is how long a forward must stay up before we consider it stable.
	pfRestoreGraceWindow = 2 * time.Second
)

// RestartExtender represents a restartable resource.
type RestartExtender struct {
	ResourceViewer
}

// NewRestartExtender returns a new extender.
func NewRestartExtender(v ResourceViewer) ResourceViewer {
	r := RestartExtender{ResourceViewer: v}
	v.AddBindKeysFn(r.bindKeys)

	return &r
}

// BindKeys creates additional menu actions.
func (r *RestartExtender) bindKeys(aa *ui.KeyActions) {
	if r.App().Config.IsReadOnly() {
		return
	}
	aa.Add(ui.KeyR, ui.NewKeyActionWithOpts("Restart", r.restartCmd,
		ui.ActionOpts{
			Visible:   true,
			Dangerous: true,
		},
	))
}

func (r *RestartExtender) restartCmd(*tcell.EventKey) *tcell.EventKey {
	paths := r.GetTable().GetSelectedItems()
	if len(paths) == 0 || paths[0] == "" {
		return nil
	}

	r.Stop()
	defer r.Start()
	msg := fmt.Sprintf("Restart %s %s?", singularize(r.GVR().R()), paths[0])
	if len(paths) > 1 {
		msg = fmt.Sprintf("Restart %d %s?", len(paths), r.GVR().R())
	}
	d := r.App().Styles.Dialog()

	// Snapshot any active port-forwards so we can offer to restore them once the
	// replacement pod(s) are ready.
	pfSnaps := r.snapshotPortForwards(paths)

	opts := dialog.RestartDialogOpts{
		Title:        "Confirm Restart",
		Message:      msg,
		FieldManager: "kubectl-rollout",
		Ack: func(opts *metav1.PatchOptions) bool {
			ctx, cancel := context.WithTimeout(context.Background(), r.App().Conn().Config().CallTimeout())
			defer cancel()
			for _, path := range paths {
				if err := r.restartRollout(ctx, path, opts); err != nil {
					r.App().Flash().Err(err)
					continue
				}
				r.App().Flash().Infof("Restart in progress for `%s...", path)
				r.maybeRetainPortForward(path, pfSnaps[path])
			}
			return true
		},
		Cancel: func() {},
	}
	dialog.ShowRestart(&d, r.App().Content.Pages, &opts)

	return nil
}

func (r *RestartExtender) restartRollout(ctx context.Context, path string, opts *metav1.PatchOptions) error {
	res, err := dao.AccessorFor(r.App().factory, r.GVR())
	if err != nil {
		return err
	}
	s, ok := res.(dao.Restartable)
	if !ok {
		return errors.New("resource is not restartable")
	}

	return s.Restart(ctx, path, opts)
}

// pfSnapshot captures the active port-forwards for a pod at restart time.
type pfSnapshot struct {
	podName string
	tunnels port.PortTunnels
}

// snapshotPortForwards records the active port-forwards for each resource about to be restarted.
func (r *RestartExtender) snapshotPortForwards(paths []string) map[string]pfSnapshot {
	snaps := make(map[string]pfSnapshot, len(paths))
	for _, path := range paths {
		podName, err := r.podForPath(path)
		if err != nil || podName == "" {
			continue
		}
		tt := r.App().factory.Forwarders().TunnelsFor(podName)
		if len(tt) == 0 {
			continue
		}
		snaps[path] = pfSnapshot{podName: podName, tunnels: tt}
	}

	return snaps
}

// maybeRetainPortForward asks the user whether the port-forward active on the pod for path
// should be re-established once its replacement pod is ready.
func (r *RestartExtender) maybeRetainPortForward(path string, snap pfSnapshot) {
	if snap.podName == "" || len(snap.tunnels) == 0 {
		return
	}

	styles := r.App().Styles.Dialog()
	msg := fmt.Sprintf("Pod %s has an active port-forward.\nKeep it running once the new pod is ready?", snap.podName)
	dialog.ShowConfirmYesNo(&styles, r.App().Content.Pages, "Keep Port-Forward", msg,
		func() {
			go r.restorePortForward(path, snap)
		},
		func() {},
	)
}

// restorePortForward waits for path's replacement pod to become ready and re-establishes
// the previously active port-forwards against it.
func (r *RestartExtender) restorePortForward(path string, snap pfSnapshot) {
	deadline := time.Now().Add(pfRestoreTimeout)
	var newPod string
	for time.Now().Before(deadline) {
		if podName, ready := r.readyPodForPath(path, snap.podName); ready {
			newPod = podName
			break
		}
		time.Sleep(pfRestorePollInterval)
	}
	if newPod == "" {
		r.App().QueueUpdateDraw(func() {
			r.App().Flash().Errf("Timed out waiting for a new pod to restore port-forward on %s", path)
		})
		return
	}

	// The container may still be starting up right after the Ready condition flips.
	time.Sleep(pfRestoreSettleDelay)

	for _, pt := range snap.tunnels {
		if _, ok := r.App().factory.ForwarderFor(dao.PortForwardID(newPod, pt.Container, pt.PortMap())); ok {
			continue
		}
		r.startRestoredForward(newPod, pt)
	}
}

// startRestoredForward (re)establishes a single port-forward tunnel on the given pod,
// retrying a few times if the forward fails right away.
func (r *RestartExtender) startRestoredForward(newPod string, pt port.PortTunnel) {
	for attempt := 1; attempt <= pfRestoreMaxAttempts; attempt++ {
		pf := dao.NewPortForwarder(r.App().factory)
		fwd, err := pf.Start(newPod, pt)
		if err != nil {
			if attempt == pfRestoreMaxAttempts {
				r.App().QueueUpdateDraw(func() {
					r.App().Flash().Errf("Unable to restore port-forward on %s: %s", newPod, err)
				})
				return
			}
			time.Sleep(pfRestoreRetryDelay)
			continue
		}

		r.App().factory.AddForwarder(pf)
		pf.SetActive(true)
		errCh := make(chan error, 1)
		go func() { errCh <- fwd.ForwardPorts() }()

		select {
		case err := <-errCh:
			r.App().factory.DeleteForwarder(pf.ID())
			pf.SetActive(false)
			if attempt == pfRestoreMaxAttempts {
				r.App().QueueUpdateDraw(func() {
					r.App().Flash().Errf("PortForward failed for %s: %s", pf.ID(), err)
				})
				return
			}
			time.Sleep(pfRestoreRetryDelay)
		case <-time.After(pfRestoreGraceWindow):
			// Forward survived the initial window; treat it as stable and hand off monitoring.
			r.App().QueueUpdateDraw(func() {
				r.App().Flash().Infof("PortForward restored %s on %s", pt.PortMap(), newPod)
			})
			go func() {
				if err := <-errCh; err != nil {
					r.App().Flash().Warnf("PortForward failed for %s: %s. Deleting!", pf.ID(), err)
				}
				r.App().QueueUpdateDraw(func() {
					r.App().factory.DeleteForwarder(pf.ID())
					pf.SetActive(false)
				})
			}()
			return
		}
	}
}

// readyPodForPath returns the current pod backing path along with whether it is a fresh,
// ready replacement for oldPod.
func (r *RestartExtender) readyPodForPath(path, oldPod string) (string, bool) {
	podName, err := r.podForPath(path)
	if err != nil || podName == "" || podName == oldPod {
		return podName, false
	}

	res, err := dao.AccessorFor(r.App().factory, client.PodGVR)
	if err != nil {
		return podName, false
	}
	po, ok := res.(*dao.Pod)
	if !ok {
		return podName, false
	}
	pod, err := po.GetInstance(podName)
	if err != nil {
		return podName, false
	}
	if pod.Status.Phase != v1.PodRunning {
		return podName, false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == v1.PodReady {
			return podName, c.Status == v1.ConditionTrue
		}
	}

	return podName, false
}

// podForPath returns the pod backing the given controller resource path.
func (r *RestartExtender) podForPath(path string) (string, error) {
	res, err := dao.AccessorFor(r.App().factory, r.GVR())
	if err != nil {
		return "", err
	}
	ctrl, ok := res.(dao.Controller)
	if !ok {
		return "", errors.New("resource is not a controller")
	}

	return ctrl.Pod(path)
}

// Helpers...

func singularize(s string) string {
	if strings.LastIndex(s, "s") == len(s)-1 {
		return s[:len(s)-1]
	}

	return s
}
