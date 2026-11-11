package matching

// Deviation (pause, library addition). Tasks of a paused fairness key stay loaded and in the
// matcher, which skips them in findMatch the same way it skips tasks of a rate-limited key. But
// the reader does not count them toward its read batch or GetTasksReloadAt. Otherwise a paused
// key with more loaded tasks than the reload threshold would stop the reader, and tasks of other
// keys behind them would never be read. Paused-key tasks share Config.MaxDelayedInMemory with
// delayed tasks: up to that many "held" tasks per subqueue do not count, and past it they count
// as loaded, which bounds memory. Resuming needs no re-injection: the tasks are still in the
// matcher, and the config change reprocesses it. With no key paused, nothing here runs.

// pausedKeys returns the queue's paused fairness keys (nil if none), from the same cached
// snapshot that findMatch uses.
func (tr *fairTaskReader) pausedKeys() map[string]struct{} {
	return tr.backlogMgr.pqMgr.PausedFairnessKeys()
}

// isPausedKeyTask reports whether task belongs to a paused fairness key.
func isPausedKeyTask(keys map[string]struct{}, task *internalTask) bool {
	if keys == nil || task.isPollForwarder() {
		return false
	}
	_, p := keys[task.getPriority().FairnessKey]
	return p
}

// pausedLoadedLocked counts the loaded (not leased, delayed or acked) tasks of paused keys.
func (tr *fairTaskReader) pausedLoadedLocked(keys map[string]struct{}) int {
	if keys == nil {
		return 0
	}
	n := 0
	tr.outstandingTasks.Scan(func(o outstandingTask) bool {
		if !o.acked() && !o.task.leased && !o.task.delayed && isPausedKeyTask(keys, o.task) {
			n++
		}
		return true
	})
	return n
}

// addLoadedLocked changes loadedTasks by delta for task, and pausedLoaded when task belongs
// to a key in countedKeys. It is the only place loadedTasks changes
// (TestLoadedTasksChangedOnlyByHelper), which keeps pausedLoaded exact without a scan.
func (tr *fairTaskReader) addLoadedLocked(task *internalTask, delta int) {
	tr.loadedTasks += delta
	if isPausedKeyTask(tr.countedKeys, task) {
		tr.pausedLoaded += delta
	}
}

// recountPausedLocked adopts the current paused keys and recounts pausedLoaded (once per
// pause change; between changes addLoadedLocked keeps it exact).
func (tr *fairTaskReader) recountPausedLocked() {
	tr.countedKeys = tr.pausedKeys()
	tr.pausedLoaded = tr.pausedLoadedLocked(tr.countedKeys)
}

// pauseChanged recounts each reader's paused-key tasks after a pause change and lets it load
// past them. A config change that left the pause snapshot as it was does nothing.
func (c *fairBacklogManagerImpl) pauseChanged() {
	c.subqueueLock.Lock()
	readers := append([]*fairTaskReader(nil), c.subqueues...)
	c.subqueueLock.Unlock()
	for _, r := range readers {
		r.lock.Lock()
		if !sameKeySet(r.countedKeys, r.pausedKeys()) {
			r.recountPausedLocked()
			r.maybeReadTasksLocked()
		}
		r.lock.Unlock()
	}
}

func sameKeySet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
