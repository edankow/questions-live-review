# questions-live-review

Deploy this Worker before publishing the matching `questions` frontend. Existing clients can still review, but reload recovery requires the new frontend and a newly created room.

Rooms last at most two hours from creation; unused invitations expire after 15 minutes. A temporary disconnect leaves the room open while the other participant remains connected. The last participant disconnecting ends the room immediately, including simultaneous reloads or a shared network outage. Each role has a private reconnect token, and acknowledged message queues preserve ratings and scenario changes across individual reloads. An explicit `leave` also closes the room for both participants.

Closed rooms discard participant state and retain an ended marker until expiry, so old links cannot recreate them. Opening an ended or expired link displays “This session has ended” with an option to start a new review.

The frontend retains the room URL and saves its role token, review state, and unfinished inputs in tab-specific session storage. Reload recovery works in that same tab; the guest invite alone does not grant access to an already claimed role.

Run server regression checks with `node --test tests/reconnect.test.cjs`. Frontend recovery checks live in the `questions` repository at `tests/live-review-reconnect.test.cjs`.
