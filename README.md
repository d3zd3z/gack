# Introduction

Gack is a small utility I've written to help me with backups.  It
doesn't do backups itself, but coordinates all of the various other
tools that are needed to do backups.  Some of the things it has to
consider and support:

  - ZFS snapshots.  ZFS snapshots can persist for a long time.  In
    addition to creating them, and using them, we also need to be able
    to clean them up over time, ideally coordinated with other backups
    made of them.

  - ZFS clones.  ZFS supports backups by cloning to another ZFS
    filesystem.  This can be done locally, or to a remote system (via
    ssh).  To be able to clone, we need to figure out what snapshots
    are present at the destination, and come up with a strategy to
    update the snapshots that are missing.

  - File integrity.  We use the 'rsure' tool to maintain a database of
    the state of files in a given tree.  This can be used to verify
    that a restore.  Some snapshots allow the surefile to be updated
    in the snapshot before the backup is made, therefore meaning that
    each snapshot will contain the current surefile.  Others (namely
    ZFS snapshots) don't allow the snapshot to be modified, and
    therefore the surefiles have to be maintained separately (and
    generally backed up on their own).

  - LVM2 snapshot.  We support using LVM2 snapshots in order to make
    robust snapshots of a given filesystem.  This allows the surefile
    and the backed-up data to be consistent, and not have corruptions
    due to modifications.

  - Non snapshots.  Some filesystems, such as /boot and EFI don't
    support snapshots, but we still want to be able to create and
    update surefiles, and perform the backup types that make sense.

  - Restic.  Restic is a backup tool that supports snapshots and
    redundancy removal.

  - Borg (borgbackup).  Borg is another tool that backs up to
    snapshots.  It seems to be more mature than restic, and restores
    have been more reliable.

  - xfsdump.  XFS filesystems can be backed up with xfsdump.  xfsdump
    has an inventory database it maintains, that can't be redirected.
    Because we want to be able to maintain multiple independent backup
    destinations, we capture this inventory in the backup destination.
    We will move the existing inventory aside, replacing it with a
    temporary inventory, then putting the existing one back after our
    backup.

It is reasonable to use multiple of these strategies for a given
volume.  For snapshots that do not persist long-term (lvm2), we need
to try and keep track of where we left things so that they can be
cleaned up.  A general run will be something like:

```
# Make appropriate snapshots.
gack snap

# Update surefiles.
gack sure

# Clone any volumes on ZFS that have clones specified.
gack clone

# Run the various subtypes
gack restic
gack borg
gack xfsdump

# Remove transient snapshots
gack unsnap
```

The tool generally does not leave snapshots mounted.  ZFS has an
automounter, but we use bind mounts temporary, since several of the
snapshot tools require the snapshot to be mounted in the same place in
order for it to detect files that haven't changed.

# Implementation status

The above is more of a goal of where we are going, rather than where
it is at.  Currently, gack is a somewhat outdated version of this
utility that works just with ZFS snapshots.
