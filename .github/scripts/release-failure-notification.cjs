// Decides whether a completed Release run deserves a Slack alert and builds
// the message. Called by release-failure-notification.yml through
// actions/github-script; adapted from posthog-python's script of the same
// name. Job names are release.yml's.
const { readdirSync, readFileSync } = require('node:fs');
const { join } = require('node:path');

const unsuccessful = new Set(['failure', 'cancelled', 'timed_out', 'action_required', 'startup_failure', 'stale']);
// The job that pushes the tags the publish workflows fire on. Once it has
// succeeded, later problems are follow-ups, not a failed release.
const tagJobs = ['Tag and release'];

const isJob = (job, name) => job.name === name || job.name.startsWith(`${name} (`);

function readThreadTimestamp(directory, attempt) {
    if (!directory) return undefined;
    try {
        const entries = readdirSync(directory);
        // A single matching artifact is extracted straight into the directory.
        // Failed-job reruns reuse the approval thread from the latest earlier attempt.
        const files = entries.includes('thread-ts.txt')
            ? [join(directory, 'thread-ts.txt')]
            : entries
                .map(name => /^release-slack-thread-(\d+)$/.exec(name)?.[1])
                .filter(value => value && Number(value) <= attempt)
                .sort((a, b) => Number(b) - Number(a))
                .map(value => join(directory, `release-slack-thread-${value}`, 'thread-ts.txt'));
        if (files.length === 0) return undefined;
        const timestamp = readFileSync(files[0], 'utf8').trim();
        return /^\d+\.\d+$/.test(timestamp) ? timestamp : undefined;
    } catch (error) {
        if (error.code !== 'ENOENT') console.error('Could not read Slack thread:', error.message);
        return undefined;
    }
}

module.exports = async function releaseFailureNotification({ github, context, channel, threadDirectory }) {
    const run = context.payload.workflow_run;
    const threadTs = readThreadTimestamp(threadDirectory, run.run_attempt);
    const destination = { channel, ...(threadTs ? { thread_ts: threadTs } : {}) };
    let jobs;
    try {
        jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRunAttempt, {
            ...context.repo,
            run_id: run.id,
            attempt_number: run.run_attempt,
            per_page: 100,
        });
    } catch (error) {
        console.error('Failed to inspect release jobs:', error.status ?? error.name, error.message);
        return {
            ...destination,
            text: [
                `⚠️ Could not inspect release jobs for \`${context.repo.owner}/${context.repo.repo}\` (attempt ${run.run_attempt}).`,
                'Please check this release manually; individual job results are unavailable.',
                `<${run.html_url}|View release logs> (run: ${run.conclusion})`,
            ].join('\n'),
            unfurl_links: false,
            unfurl_media: false,
        };
    }

    if (jobs.length === 0 && run.conclusion === 'cancelled') {
        return null;
    }

    // A run with nothing to release (the release commit itself re-triggers
    // the workflow by deleting changesets) has only the check job.
    const checkOnly = jobs.length === 1 && jobs[0].name === 'Check for changesets' && jobs[0].conclusion === 'success';
    if (checkOnly) {
        return null;
    }

    const failedJobs = jobs.filter(job => unsuccessful.has(job.conclusion));
    const versionBump = jobs.find(job => job.name === 'Bump versions and commit to main');
    // An explicit rejection at the approval gate: the bump job "fails" with
    // no steps, and the rejected reply was posted. Not an alert.
    if (run.conclusion === 'failure' && failedJobs.length === 1
        && versionBump?.conclusion === 'failure' && versionBump.steps?.length === 0
        && jobs.some(job => job.name === 'Notify Slack - Rejected'
            && job.steps?.some(step => step.name === 'Notify Slack - Rejected' && step.conclusion === 'success'))) {
        return null;
    }

    const tagged = tagJobs.every(name => {
        const cells = jobs.filter(job => isJob(job, name));
        return cells.length > 0 && cells.every(job => job.conclusion === 'success');
    });
    const releasedNotification = jobs.find(job => job.name === 'Notify Slack - Released');
    const problems = failedJobs.map(job => `• <${job.html_url}|${job.name}>: ${job.conclusion}`);

    if (run.conclusion !== 'success' && failedJobs.length === 0) {
        for (const name of tagJobs) {
            if (!jobs.some(job => isJob(job, name))) problems.push(`• ${name}: never ran`);
        }
    }

    if (tagged && (!releasedNotification || releasedNotification.conclusion === 'skipped')) {
        problems.push('• The released Slack notification did not run.');
    }

    if (problems.length === 0 && run.conclusion === 'success') {
        return null;
    }

    return {
        ...destination,
        text: [
            tagged
                ? `⚠️ Release tagged; follow-up problems for \`${context.repo.owner}/${context.repo.repo}\` (attempt ${run.run_attempt}). The PyPI publish runs in publish-pyhoglake.yml and publish-pyhoglake-variant-codec.yml; check those too.`
                : `❌ Release process failed or was cancelled for \`${context.repo.owner}/${context.repo.repo}\` (attempt ${run.run_attempt}).`,
            ...problems,
            `<${run.html_url}|View release logs> (run: ${run.conclusion})`,
        ].join('\n'),
        unfurl_links: false,
        unfurl_media: false,
    };
};
