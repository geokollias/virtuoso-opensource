--
--  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
--  project.
--
--  Copyright (C) 1998-2015 OpenLink Software
--
--  This project is free software; you can redistribute it and/or modify it
--  under the terms of the GNU General Public License as published by the
--  Free Software Foundation; only version 2 of the License, dated June 1991.
--
--  This program is distributed in the hope that it will be useful, but
--  WITHOUT ANY WARRANTY; without even the implied warranty of
--  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
--  General Public License for more details.
--
--  You should have received a copy of the GNU General Public License along
--  with this program; if not, write to the Free Software Foundation, Inc.,
--  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
--

use DB
;

--| This matches DAV_AUTHENTICATE (in id any, in what char(1), in req varchar, in a_uname varchar, in a_pwd varchar, in a_uid integer := null)
--| The difference is that the DET function should not check whether the pair of name and password is valid; the auth_uid is not a null already.
create function "IMAP_DAV_AUTHENTICATE" (
  in id any,
  in what char(1),
  in req varchar,
  in a_uname varchar,
  in a_pwd varchar,
  in a_uid integer)
{
  -- dbg_obj_princ ('IMAP_DAV_AUTHENTICATE (', id, what, req, a_uname, a_pwd, a_uid, ')');
  declare rc integer;

  rc := DAV_AUTHENTICATE (id[1], 'C', req, a_uname, a_pwd, a_uid);
  if (rc >= 0)
    return rc;

  if (DAV_AUTHENTICATE_SSL_CONDITION ())
  {
    declare _perms, a_gid any;

    if (DAV_AUTHENTICATE_SSL (id, what, null, req, a_uid, a_gid, _perms))
      return a_uid;
  }
  return -12;
}
;

--| This exactly matches DAV_AUTHENTICATE_HTTP (in id any, in what char(1), in req varchar, in can_write_http integer, inout a_lines any, inout a_uname varchar, inout a_pwd varchar, inout a_uid integer, inout a_gid integer, inout _perms varchar) returns integer
--| The function should fully check access because DAV_AUTHENTICATE_HTTP do nothing with auth data either before or after calling this DET function.
--| Unlike DAV_AUTHENTICATE, user name passed to DAV_AUTHENTICATE_HTTP header may not match real DAV user.
--| If DET call is successful, DAV_AUTHENTICATE_HTTP checks whether the user have read permission on mount point collection.
--| Thus even if DET function allows anonymous access, the whole request may fail if mountpoint is not readable by public.
create function "IMAP_DAV_AUTHENTICATE_HTTP" (
  in id any,
  in what char(1),
  in req varchar,
  in can_write_http integer,
  inout a_lines any,
  inout a_uname varchar,
  inout a_pwd varchar,
  inout a_uid integer,
  inout a_gid integer,
  inout _perms varchar) returns integer
{
  -- dbg_obj_princ ('IMAP_DAV_AUTHENTICATE_HTTP (', id, what, req, can_write_http, a_lines, a_uname, a_pwd, a_uid, a_gid, _perms, ')');
  declare rc integer;

  rc := DAV_AUTHENTICATE_HTTP (id[1], 'C', req, can_write_http, a_lines, a_uname, a_pwd, a_uid, a_gid, _perms);
  if (rc >= 0)
    return rc;

  if (DAV_AUTHENTICATE_SSL_CONDITION ())
  {
    if (DAV_AUTHENTICATE_SSL (id, what, null, req, a_uid, a_gid, _perms))
      return a_uid;
  }
  return -12;
}
;

--| This should return ID of the collection that contains resource or collection with given ID,
--| Possible ambiguity (such as symlinks etc.) should be resolved by using path.
--| This matches DAV_GET_PARENT (in id any, in st char(1), in path varchar) returns any
create function "IMAP_DAV_GET_PARENT" (
  in id any,
  in what char(1),
  in path varchar) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_GET_PARENT (', id, what, path, ')');
  declare _id integer;
  declare owner varchar;

  _id := null;
  owner := DB.DBA.IMAP__owner (id);
  if (what = 'R')
  {
    what := 'C';
    _id := (select MM_FLD_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = id[2]);
  }
  else if (what = 'C')
  {
    _id := (select MF_PARENT_ID from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_ID = id[2]);
    if (not isnull (_id))
    {
      if (_id = 0)
        return id[1];
    }
  }
  if (not isnull (_id))
    return vector (UNAME'IMAP', id[1], _id, what);

  return -14;
}
;

--| When DAV_COL_CREATE_INT calls DET function, authentication, check for lock and check for overwrite are passed, uid and gid are translated from strings to IDs.
--| Check for overwrite, but the deletion of previously existing collection should be made by DET function.
create function "IMAP_DAV_COL_CREATE" (
  in detcol_id any,
  in path_parts any,
  in permissions varchar,
  in uid integer,
  in gid integer,
  in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_COL_CREATE (', detcol_id, path_parts, permissions, uid, gid, auth_uid, ')');
  declare rc integer;
  declare owner varchar;
  declare params, retValue any;

  owner := DB.DBA.IMAP__owner (detcol_id);
  params := DB.DBA.IMAP__params (detcol_id);
  retValue := DB.DBA.IMAP__folderIMAPCreate (detcol_id, owner, params, path_parts);
  if (retValue)
    return retValue;

  return -20;
}
;

--| It looks like that this is redundant and should be removed at all.
create function "IMAP_DAV_COL_MOUNT" (in detcol_id any, in path_parts any, in full_mount_path varchar, in mount_det varchar, in permissions varchar, in uid integer, in gid integer, in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_COL_MOUNT (', detcol_id, path_parts, full_mount_path, mount_det, permissions, uid, gid, auth_uid, ')');
  return -20;
}
;

--| It looks like that this is redundant and should be removed at all.
create function "IMAP_DAV_COL_MOUNT_HERE" (in parent_id any, in full_mount_path varchar, in permissions varchar, in uid integer, in gid integer, in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_COL_MOUNT_HERE (', parent_id, full_mount_path, permissions, uid, gid, auth_uid, ')');
  return -20;
}
;

--| When DAV_DELETE_INT calls DET function, authentication and check for lock are passed.
create function "IMAP_DAV_DELETE" (
  in detcol_id any,
  in path_parts any,
  in what char(1),
  in silent integer,
  in auth_uid integer) returns integer
{
  -- dbg_obj_princ ('IMAP_DAV_DELETE (', detcol_id, path_parts, what, silent, auth_uid, ')');
  declare owner varchar;
  declare params, retValue any;

  retValue := -20;
  owner := DB.DBA.IMAP__owner (detcol_id);
  params := DB.DBA.IMAP__params (detcol_id);
  if (what = 'C')
  {
    retValue := DB.DBA.IMAP__folderIMAPErase (detcol_id, owner, params, path_parts);
  }
  else if (what = 'R')
  {
    retValue := DB.DBA.IMAP__messageIMAPErase (detcol_id, owner, params, path_parts);
  }
  return retValue;
}
;

--| When DAV_RES_UPLOAD_STRSES_INT calls DET function, authentication and check for locks are performed before the call.
--| There's a special problem, known as 'Transaction deadlock after reading from HTTP session'.
--| The DET function should do only one INSERT of the 'content' into the table and do it as late as possible.
--| The function should return -29 if deadlocked or otherwise broken after reading blob from HTTP.
create function "IMAP_DAV_RES_UPLOAD" (in detcol_id any, in path_parts any, inout content any, in type varchar, in permissions varchar, in uid integer, in gid integer, in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_RES_UPLOAD (', detcol_id, path_parts, ', [content], ', type, permissions, uid, gid, auth_uid, ')');
  return -20;
}
;

--| When DAV_PROP_REMOVE_INT calls DET function, authentication and check for locks are performed before the call.
--| The check whether it's a system name or not (when an error in returned if name is system) is _not_ permitted.
--| It should delete any dead property even if the name looks like system name.
create function "IMAP_DAV_PROP_REMOVE" (
  in id any,
  in what char(0),
  in propname varchar,
  in silent integer,
  in auth_uid integer) returns integer
{
  -- dbg_obj_princ ('IMAP_DAV_PROP_REMOVE (', id, what, propname, silent, auth_uid, ')');
  if (propname = 'virt:aci_meta')
  {
    declare owner varchar;

    owner := DB.DBA.IMAP__owner (id);
    if ('R' = what)
    {
      update DB.DBA.MAIL_MESSAGE
         set MM_ACL = null
       where MM_OWN = owner
         and MM_ID = id[2];
    }
    else
    {
      update DB.DBA.MAIL_FOLDER
         set MF_ACL = null
       where MF_OWN = owner
         and MF_ID = id[2];
    }
    return 1;
  }
  return -20;
}
;

--| When DAV_PROP_SET_INT calls DET function, authentication and check for locks are performed before the call.
--| The check whether it's a system property or not is _not_ permitted and the function should return -16 for live system properties.
create function "IMAP_DAV_PROP_SET" (
  in id any,
  in what char(0),
  in propname varchar,
  in propvalue any,
  in overwrite integer,
  in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_PROP_SET (', id, what, propname, propvalue, overwrite, auth_uid, ')');
  if (propname = 'virt:aci_meta_n3')
  {
    declare owner varchar;

    owner := DB.DBA.IMAP__owner (id);
    if ('R' = what)
    {
      update DB.DBA.MAIL_MESSAGE
         set MM_ACL = propvalue
       where MM_OWN = owner
         and MM_ID = id[2];
    }
    else
    {
      update DB.DBA.MAIL_FOLDER
         set MF_ACL = propvalue
       where MF_OWN = owner
         and MF_ID = id[2];
    }
    return 1;
  }
  if (propname[0] = 58)
    return -16;

  return -20;
}
;

--| When DAV_PROP_GET_INT calls DET function, authentication and check whether it's a system property are performed before the call.
create function "IMAP_DAV_PROP_GET" (
  in id any,
  in what char(0),
  in propname varchar,
  in auth_uid integer)
{
  -- dbg_obj_princ ('IMAP_DAV_PROP_GET (', id, what, propname, auth_uid, ')');
  if ('virt:aci_meta_n3' = propname)
  {
    declare owner varchar;

    owner := DB.DBA.IMAP__owner (id);
    if ('R' = what)
    {
      return (select MM_ACL from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = id[2]);
    }
    else if ('C' = what)
    {
      return (select MF_ACL from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_ID = id[2]);
    }
  }
  return -11;
}
;

--| When DAV_PROP_LIST_INT calls DET function, authentication is performed before the call.
--| The returned list should contain only user properties.
create function "IMAP_DAV_PROP_LIST" (
  in id any,
  in what char(0),
  in propmask varchar,
  in auth_uid integer)
{
  -- dbg_obj_princ ('IMAP_DAV_PROP_LIST (', id, what, propmask, auth_uid, ')');
  return vector ();
}
;

--| When DAV_PROP_GET_INT or DAV_DIR_LIST_INT calls DET function, authentication is performed before the call.
create function "IMAP_DAV_DIR_SINGLE" (
  in id any,
  in what char(0),
  in path any,
  in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_DIR_SINGLE (', id, what, path, auth_uid, ')');
  declare owner varchar;
  declare imapEntry, detEntry any;

  owner := DB.DBA.IMAP__owner (id);
  detEntry := DB.DBA.DAV_DIR_SINGLE_INT (id[1], 'C', '', null, null, http_dav_uid ());
  imapEntry := vector();
  if (what = 'C')
  {
    for (select * from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_ID = id[2] and MF_IS_DELETED <> 1) do
    {
      imapEntry := vector (
                     detEntry[0] || DB.DBA.IMAP__folderPath (owner, MF_ID) || '/',
                     'C',
                     0,
                     detEntry[3],
                      vector (UNAME'IMAP', id[1], MF_ID, 'C'),
                     detEntry[5],
                     detEntry[6],
                     detEntry[7],
                     detEntry[8],
                     'dav/unix-directory',
                     MF_NAME);
    }
  }
  else if (what = 'R')
  {
    for (select * from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = id[2] and MM_IS_DELETED <> 1) do
    {
      imapEntry := vector (
                     detEntry[0] || DB.DBA.IMAP__folderPath (owner, MM_FLD_ID) || '/' || DB.DBA.IMAP__nameCompose (MM_ID, MM_SUBJ),
                     'R',
                     length (MM_BODY),
                     DB.DBA.IMAP__dateConvert (MM_SND_TIME),
                      vector (UNAME'IMAP', id[1], MM_ID, 'R'),
                     detEntry[5],
                     detEntry[6],
                     detEntry[7],
                     DB.DBA.IMAP__dateConvert (MM_SND_TIME),
                     'message/rfc822',
                     DB.DBA.IMAP__nameCompose (MM_ID, MM_SUBJ));
    }
  }
  return imapEntry;
}
;

--| When DAV_PROP_GET_INT or DAV_DIR_LIST_INT calls DET function, authentication is performed before the call.
create function "IMAP_DAV_DIR_LIST" (
  in detcol_id any,
  in path_parts any,
  in detcol_path any,
  in name_mask varchar,
  in recursive integer,
  in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_DIR_LIST (', detcol_id, path_parts, detcol_path, name_mask, recursive, auth_uid, ')');
  declare exit handler for sqlstate '*'
  {
    --dbg_obj_print ('', __SQL_STATE, __SQL_MESSAGE);
    ;
  };
  declare owner, what varchar;
  declare params, save, retValue any;

  what := case when ((length (path_parts) = 0) or (path_parts[length (path_parts) - 1] = '')) then 'C' else 'R' end;
  if ((what = 'R') or (recursive = -1))
    return DB.DBA.IMAP_DAV_DIR_SINGLE (detcol_id, what, null, auth_uid);

  owner := DB.DBA.IMAP__owner (detcol_id);
  params := DB.DBA.IMAP__params (detcol_id);
  save := connection_get ('dav_store');
  if (save is null)
  {
    DB.DBA.IMAP__load (detcol_id, owner, params, path_parts);
  }

  retValue := DB.DBA.IMAP__davList (detcol_id, owner, params, path_parts);
  return retValue;
}
;

--| When DAV_DIR_FILTER_INT calls DET function, authentication is performed before the call and compilation is initialized.
create function "IMAP_DAV_DIR_FILTER" (
  in detcol_id any,
  in path_parts any,
  in detcol_path varchar,
  inout compilation any,
  in recursive integer,
  in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_DIR_FILTER (', detcol_id, path_parts, detcol_path, compilation, recursive, auth_uid, ')');
  declare owner, folderPath varchar;
  declare folderId integer;
  declare detEntry, retValue any;
  declare sql, st, msg, meta, rows any;

  owner := DB.DBA.IMAP__owner (detcol_id);
  vectorbld_init (retValue);
  if ((length (path_parts) <= 1) and (recursive <> 1))
    goto _exit;

  detEntry := DB.DBA.DAV_DIR_SINGLE_INT (detcol_id, 'C', '', null, null, http_dav_uid ());
  folderId := coalesce (DB.DBA.IMAP__folderId (owner, path_parts), 0);
  folderPath := DB.DBA.IMAP__path (owner, folderId, 'C');
  sql :=
    'select _param.detEntry[0] || DB.DBA.IMAP__folderPath (_message.MM_OWN, _message.MM_FLD_ID) || ''/'' || DB.DBA.IMAP__nameCompose (_message.MM_ID, _message.MM_SUBJ),
            ''R'',
            length (_message.MM_BODY),
            DB.DBA.IMAP__dateConvert (_message.MM_SND_TIME),
            vector (UNAME''IMAP'', ?, _message.MM_ID, ''R''),
            _param.detEntry[5],
            _param.detEntry[6],
            _param.detEntry[7],
            DB.DBA.IMAP__dateConvert (_message.MM_SND_TIME),
            ''message/rfc822'',
            DB.DBA.IMAP__nameCompose (_message.MM_ID, _message.MM_SUBJ)
       from (select top 1 ? as detEntry from WS.WS.SYS_DAV_COL) as _param,
            DB.DBA.MAIL_MESSAGE as _message' ||
              --join WS.WS.SYS_DAV_USER as _owner on _owner.U_NAME = _message.MM_OWN' ||
    DB.DBA.IMAP_DAV_FC_PRINT_WHERE (get_keyword ('', compilation), auth_uid) ||
    'and MM_OWN = \'' || owner || '\' and DB.DBA.IMAP__path (_message.MM_OWN, _message.MM_ID, ''R'') between ? and ?';

  st := '00000';
  exec (sql, st, msg, vector (detcol_id, detEntry, folderPath, DB.DBA.DAV_COL_PATH_BOUNDARY (folderPath)), 0, meta, rows);
  if ('00000' <> st)
    signal (st, msg || ' in ' || sql);

  vectorbld_concat_acc (retValue, rows);
_exit: ;
  vectorbld_final (retValue);
  return retValue;
}
;

create procedure "IMAP_DAV_FC_PRED_METAS" (
  inout pred_metas any)
{
  pred_metas := vector (
    'RES_ID',            vector ('MAIL_MESSAGE',  0, 'integer',  'MM_ID'),
    'RES_NAME',          vector ('MAIL_MESSAGE',  0, 'varchar',  'DB.DBA.IMAP__nameCompose (MM_ID, MM_SUBJ)'),
    'RES_FULL_PATH',     vector ('MAIL_MESSAGE',  0, 'varchar',  'DB.DBA.IMAP__path (MM_OWN, MM_ID, ''R'')'),
    'RES_TYPE',          vector ('MAIL_MESSAGE',  0, 'varchar',  '(''message/rfc822'')'),
    'RES_OWNER_ID',      vector ('SYS_DAV_USERS', 0, 'integer',  'U_ID'),
    'RES_OWNER_NAME',    vector ('SYS_DAV_USERS', 0, 'varchar',  'U_NAME'),
    'RES_GROUP_ID',      vector ('MAIL_MESSAGE',  0, 'integer',  'http_nogroup_gid()'),
    'RES_GROUP_NAME',    vector ('MAIL_MESSAGE',  0, 'varchar',  '(''nogroup'')'),
    'RES_COL_FULL_PATH', vector ('MAIL_MESSAGE',  0, 'varchar',  'DB.DBA.IMAP__path (MM_OWN, MM_FLD_ID, ''C'')'),
    'RES_COL_NAME',      vector ('MAIL_MESSAGE',  0, 'varchar',  'DB.DBA.IMAP__folderName (MM_OWN, MM_FLD_ID)'),
    'RES_CR_TIME',       vector ('MAIL_MESSAGE',  0, 'datetime', 'coalesce (MM_REC_DATE, MM_SND_TIME)'),
    'RES_MOD_TIME',      vector ('MAIL_MESSAGE',  0, 'datetime', 'coalesce (MM_REC_DATE, MM_SND_TIME)'),
    'RES_PERMS',         vector ('MAIL_MESSAGE',  0, 'varchar',  '(''110000000RR'')'),
    'RES_CONTENT',       vector ('MAIL_MESSAGE',  0, 'text',     'MM_BODY'),
    'PROP_NAME',         vector ('MAIL_MESSAGE',  0, 'varchar',  '(''Content'')'),
    'PROP_VALUE',        vector ('MAIL_MESSAGE',  1, 'text',     'MM_BODY'),
    'RES_TAGS',          vector ('MAIL_MESSAGE',  0, 'varchar',  'MM_TAGS'),
    'RES_PUBLIC_TAGS',   vector ('MAIL_MESSAGE',  0, 'varchar',  '('''')'),
    'RES_PRIVATE_TAGS',  vector ('MAIL_MESSAGE',  0, 'varchar' , '('''')'),
    'RDF_PROP',          vector ('MAIL_MESSAGE',  1, 'varchar',  NULL),
    'RDF_VALUE',         vector ('MAIL_MESSAGE',  2, 'XML',      NULL),
    'RDF_OBJ_VALUE',     vector ('MAIL_MESSAGE',  3, 'XML',      NULL)
  );
}
;

create procedure "IMAP_DAV_FC_TABLE_METAS" (
  inout table_metas any)
{
  table_metas := vector (
    'MAIL_MESSAGE', vector ('',
                            '',
                            'MM_BODY',
                            'MM_BODY',
                            '[__quiet] /'),
    'MAIL_FOLDER' , vector ('\n  inner join DB.DBA.MAIL_FOLDER as ^{alias}^ on ((^{alias}^.MF_OWN = _top.MM_OWN) and (^{alias}^.MF_ID = _top.MM_FLD_ID)^{andpredicates}^)',
                            '\n  exists (select 1 from DB.DBA.MAIL_FOLDER as ^{alias}^ where ((^{alias}^.MF_OWN = _top.MM_OWN) and (^{alias}^.MF_ID = _top.MM_FLD_ID)^{andpredicates}^)'  ,
                            NULL,
                            NULL,
                            NULL),
    'SYS_DAV_USER', vector ('\n  left outer join WS.WS.SYS_DAV_USER as ^{alias}^ on ((^{alias}^.U_NAME = _top.MM_OWN)^{andpredicates}^)'  ,
                            '\n  exists (select 1 from WS.WS.SYS_DAV_USER as ^{alias}^ where (^{alias}^.U_NAME = _top.MM_OWN)^{andpredicates}^)',
                            NULL,
                            NULL,
                            NULL)
  );
}
;

create function "IMAP_DAV_FC_PRINT_WHERE" (
  inout filter any,
  in param_uid integer) returns varchar
{
  declare pred_metas, cmp_metas, table_metas any;
  declare used_tables any;

  DB.DBA.IMAP_DAV_FC_PRED_METAS (pred_metas);
  DB.DBA.DAV_FC_CMP_METAS (cmp_metas);
  DB.DBA.IMAP_DAV_FC_TABLE_METAS (table_metas);
  used_tables := vector (
    'MAIL_MESSAGE', vector ('MAIL_MESSAGE', '_message', null, vector (), vector (), vector ()),
    'SYS_DAV_USER', vector ('SYS_DAV_USER', '_owner',   null, vector (), vector (), vector ())
  );
  return DB.DBA.DAV_FC_PRINT_WHERE_INT (filter, pred_metas, cmp_metas, table_metas, used_tables, param_uid);
}
;

--| When DAV_PROP_GET_INT or DAV_DIR_LIST_INT calls DET function, authentication is performed before the call.
create function "IMAP_DAV_SEARCH_ID" (
  in detcol_id any,
  in path_parts any,
  in what char(1)) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_SEARCH_ID (', detcol_id, path_parts, what, ')');
  declare id, folderId integer;
  declare owner varchar;

  id := null;
  owner := DB.DBA.IMAP__owner (detcol_id);
  if (what = 'C')
  {
    id := DB.DBA.IMAP__folderId (owner, path_parts);
  }
  else if (what = 'R')
  {
    folderId := DB.DBA.IMAP__folderId (owner, path_parts);
    if (isnull (folderId))
      return -1;

    id := (select MM_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_PARENT_ID = 0 and MM_FLD_ID = folderId and DB.DBA.IMAP__nameCompose (MM_ID, MM_SUBJ) = path_parts[length(path_parts)-1]);
  }
  if (isnull (id))
    return -1;

  return vector (UNAME'IMAP', detcol_id, id, what);
}
;

--| When DAV_SEARCH_PATH_INT calls DET function, authentication is performed before the call.
create function "IMAP_DAV_SEARCH_PATH" (in id any, in what char(1)) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_SEARCH_PATH (', id, what, ')');
  declare owner, detPath varchar;

  owner := DB.DBA.IMAP__owner (id);
  detPath := DB.DBA.DAV_SEARCH_PATH (id[1], 'C');
  if (what = 'C')
    return detPath || DB.DBA.IMAP__folderPath (owner, id[2]) || '/';

  if (what = 'R')
    for (select MM_ID, MM_FLD_ID, MM_SUBJ from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = id[2] and MM_IS_DELETED <> 1) do
      return detPath || DB.DBA.IMAP__folderPath (owner, MM_FLD_ID) || '/' || DB.DBA.IMAP__nameCompose (MM_ID, MM_SUBJ);

  return NULL;
}
;

--| When DAV_COPY_INT calls DET function, authentication and check for locks are performed before the call, but no check for existing/overwrite.
create function "IMAP_DAV_RES_UPLOAD_COPY" (in detcol_id any, in path_parts any, in source_id any, in what char(1), in overwrite_flags integer, in permissions varchar, in uid integer, in gid integer, in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_RES_UPLOAD_COPY (', detcol_id, path_parts, source_id, what, overwrite_flags, permissions, uid, gid, auth_uid, ')');
  return -20;
}
;

--| When DAV_COPY_INT calls DET function, authentication and check for locks are performed before the call, but no check for existing/overwrite.
create function "IMAP_DAV_RES_UPLOAD_MOVE" (in detcol_id any, in path_parts any, in source_id any, in what char(1), in overwrite_flags integer, in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_RES_UPLOAD_MOVE (', detcol_id, path_parts, source_id, what, overwrite_flags, auth_uid, ')');
  return -20;
}
;

--| When DAV_RES_CONTENT or DAV_RES_COPY_INT or DAV_RES_MOVE_INT calls DET function, authentication is made.
--| If content_mode is 1 then content is a valid output stream before the call.
create function "IMAP_DAV_RES_CONTENT" (
  in id any,
  inout content any,
  out type varchar,
  in content_mode integer) returns integer
{
  -- dbg_obj_princ ('IMAP_DAV_RES_CONTENT (', id, ', [content], [type], ', content_mode, ')');
  declare owner varchar;

  type := 'message/822';
  owner := DB.DBA.IMAP__owner (id);
  if ((content_mode = 0) or (content_mode = 1))
  {
    content := DB.DBA.IMAP__messageContent (owner, id[2]);
  }
  else if (content_mode = 2)
  {
    declare tmp any;

    tmp := DB.DBA.IMAP__messageContent (owner, id[2]);
    http (tmp, content);
  }
  else if (content_mode = 3)
  {
    declare tmp any;

    tmp := DB.DBA.IMAP__messageContent (owner, id[2]);
    http (tmp);
  }
  return id;
}
;

--| This adds an extra access path to the existing resource or collection.
create function "IMAP_DAV_SYMLINK" (in detcol_id any, in path_parts any, in source_id any, in what char(1), in overwrite integer, in uid integer, in gid integer, in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_SYMLINK (', detcol_id, path_parts, source_id, overwrite, uid, gid, auth_uid, ')');
  return -20;
}
;

--| This gets a list of resources and/or collections as it is returned by DAV_DIR_LIST and and writes the list of quads (old_id, 'what', old_full_path, dereferenced_id, dereferenced_full_path).
create function "IMAP_DAV_DEREFERENCE_LIST" (in detcol_id any, inout report_array any) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_DEREFERENCE_LIST (', detcol_id, report_array, ')');
  return -20;
}
;

--| This gets one of reference quads returned by ..._DAV_REREFERENCE_LIST() and returns a record (new_full_path, new_dereferenced_full_path, name_may_vary).
create function "IMAP_DAV_RESOLVE_PATH" (in detcol_id any, inout reference_item any, inout old_base varchar, inout new_base varchar) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_RESOLVE_PATH (', detcol_id, reference_item, old_base, new_base, ')');
  return -20;
}
;

--| There's no API function to lock for a while (do we need such?) The "LOCK" DAV method checks that all parameters are valid but does not check for existing locks.
create function "IMAP_DAV_LOCK" (in path any, in id any, in type char(1), inout locktype varchar, inout scope varchar, in token varchar, inout owner_name varchar, inout owned_tokens varchar, in depth varchar, in timeout_sec integer, in auth_uid integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_LOCK (', path, id, type, locktype, scope, token, owner_name, owned_tokens, depth, timeout_sec, auth_uid, ')');
  return -20;
}
;


--| There's no API function to unlock for a while (do we need such?) The "UNLOCK" DAV method checks that all parameters are valid but does not check for existing locks.
create function "IMAP_DAV_UNLOCK" (in id any, in type char(1), in token varchar, in auth_uid integer)
{
  -- dbg_obj_princ ('IMAP_DAV_UNLOCK (', id, type, token, auth_uid, ')');
  return -27;
}
;

--| The caller does not check if id is valid.
--| This returns -1 if id is not valid, 0 if all existing locks are listed in owned_tokens whitespace-delimited list, 1 for soft 2 for hard lock.
create function "IMAP_DAV_IS_LOCKED" (inout id any, inout type char(1), in owned_tokens varchar) returns integer
{
  -- dbg_obj_princ ('IMAP_DAV_IS_LOCKED (', id, type, owned_tokens, ')');
  return 0;
}
;

--| The caller does not check if id is valid.
--| This returns -1 if id is not valid, list of tuples (LOCK_TYPE, LOCK_SCOPE, LOCK_TOKEN, LOCK_TIMEOUT, LOCK_OWNER, LOCK_OWNER_INFO) otherwise.
create function "IMAP_DAV_LIST_LOCKS" (in id any, in type char(1), in recursive integer) returns any
{
  -- dbg_obj_princ ('IMAP_DAV_LIST_LOCKS" (', id, type, recursive);
  return vector ();
}
;

-------------------------------------------------------------------------------
--
create function "IMAP_DAV_SCHEDULER" (
  in queue_id integer,
  in detcol_id integer := null)
{
  -- dbg_obj_princ ('DB.DBA.IMAP_DAV_SCHEDULER (', queue_id, ')');
  for (select COL_ID from WS.WS.SYS_DAV_COL where COL_DET = 'IMAP' and (detcol_id is null or (detcol_id = COL_ID))) do
  {
    if (coalesce (DB.DBA.IMAP__paramGet (COL_ID, 'C', 'syncEnabled', 0), 'on') = 'on')
    {
      "IMAP_DAV_SCHEDULER_FOLDER" (queue_id, COL_ID, vector (''));
    }
  }
  DB.DBA.DAV_QUEUE_UPDATE_STATE (queue_id, 2);
}
;

-------------------------------------------------------------------------------
--
create function "IMAP_DAV_SCHEDULER_FOLDER" (
  in queue_id integer,
  in detcol_id integer,
  in path_parts any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP_DAV_SCHEDULER_FOLDER (', queue_id, detcol_id, path_parts, ')');
  declare folder_id integer;
  declare owner varchar;
  declare params any;

  owner := DB.DBA.IMAP__owner (detcol_id);
  params := DB.DBA.IMAP__params (detcol_id);
  DB.DBA.IMAP__folderIMAPSelect (queue_id, detcol_id, owner, params, path_parts);

  folder_id := coalesce (DB.DBA.IMAP__folderId (owner, path_parts), 0);
  for (select MF_NAME from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_PARENT_ID = folder_id) do
  {
    "IMAP_DAV_SCHEDULER_FOLDER" (queue_id, detcol_id, vector_concat (subseq (path_parts, 0, length (path_parts)-1), vector (MF_NAME, '')));
  }
}
;

-------------------------------------------------------------------------------
--
create function "IMAP_DAV_SCHEDULER_ROOT" (
  in detcol_id integer)
{
  -- dbg_obj_princ ('DB.DBA.IMAP_DAV_SCHEDULER_ROOT (', detcol_id, ')');

  set_user_id ('dba');
  DB.DBA.DAV_QUEUE_ADD (DB.DBA.IMAP__detName () || '_' || cast (detcol_id as varchar), 0, 'DB.DBA.IMAP_DAV_SCHEDULER', vector (detcol_id), 1);
  DB.DBA.DAV_QUEUE_INIT ();

  return 1;
}
;

-------------------------------------------------------------------------------
--
create function "IMAP_CONFIGURE" (
  in id integer,
  in params any)
{
  -- dbg_obj_princ ('IMAP_CONFIGURE (', id, params, ')');
  declare syncEnabled varchar;

  if (not isnull ("IMAP_VERIFY" (DB.DBA.DAV_SEARCH_PATH (id, 'C'), params)))
    return -38;

  -- Activity
  DB.DBA.IMAP__paramSet (id, 'C', 'activity',       get_keyword ('activity', params, 'off'), 0);

  -- Check Interval
  DB.DBA.IMAP__paramSet (id, 'C', 'checkInterval',  get_keyword ('checkInterval', params, '15'), 0);

  -- Enable/Disable sync
  syncEnabled := get_keyword ('syncEnabled', params, 'on');
  DB.DBA.IMAP__paramSet (id, 'C', 'syncEnabled',    syncEnabled, 0);

  -- RDF Graph & Sponger params
  DB.DBA.DAV_DET_RDF_PARAMS_SET ('IMAP', id, params);

  -- Access params
  DB.DBA.IMAP__paramSet (id, 'C', 'connection',     get_keyword ('connection', params), 0);
  DB.DBA.IMAP__paramSet (id, 'C', 'server',         get_keyword ('server', params), 0);
  DB.DBA.IMAP__paramSet (id, 'C', 'port',           get_keyword ('port', params), 0);
  DB.DBA.IMAP__paramSet (id, 'C', 'user',           get_keyword ('user', params), 0);
  if (get_keyword ('password', params, '') <> '**********')
    DB.DBA.IMAP__paramSet (id, 'C', 'password',     get_keyword ('password', params), 0);

  DB.DBA.IMAP__paramSet (id, 'C', 'folder',         get_keyword ('folder', params), 0);

  -- set DET Type Value
  DB.DBA.IMAP__paramSet (id, 'C', ':virtdet', DB.DBA.IMAP__detName (), 0, 0, 0);

  -- start sync scheduler
  if (syncEnabled = 'on')
    DB.DBA."IMAP_DAV_SCHEDULER_ROOT" (id);
}
;

-------------------------------------------------------------------------------
--
create function "IMAP_VERIFY" (
  in path integer,
  in params any)
{
  -- dbg_obj_princ ('IMAP_VERIFY (', path, params, ')');
  declare detcol_id integer;
  declare tmp, _path, _params any;
  declare retValue any;
  declare exit handler for sqlstate '*'
  {
    return __SQL_MESSAGE;
  };

  _params := vector ();
  detcol_id := DB.DBA.DAV_SEARCH_ID (path, 'C');

  VALIDATE.DBA.validate (get_keyword ('checkInterval', params, '15'), vector ('name', 'Check Interval', 'class', 'integer', 'minValue', 1));
  VALIDATE.DBA.validate (get_keyword ('server', params), vector ('name', 'IMAP Server', 'class', 'varchar', 'minLength', 1, 'maxLength', 255));
  VALIDATE.DBA.validate (get_keyword ('port', params), vector ('name', 'IMAP Port', 'class', 'integer', 'minLength', 1, 'maxLength', 4));
  VALIDATE.DBA.validate (get_keyword ('user', params), vector ('name', 'IMAP User', 'class', 'varchar', 'minLength', 1, 'maxLength', 255));

  tmp := get_keyword ('password', params, '**********');
  if ((tmp = '**********') and (DAV_HIDE_ERROR (detcol_id) is not null))
    tmp := DB.DBA.IMAP__paramGet (detcol_id, 'C', 'password', 0);

  retValue := DB.DBA.IMAP__verify (
    get_keyword ('connection', params),
    get_keyword ('server', params),
    get_keyword ('port', params),
    get_keyword ('user', params),
    tmp
  );
  if (retValue  <> '')
    return retValue;

  return null;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__detName ()
{
  return UNAME'IMAP';
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__params (
  in colId integer)
{
  declare tmp, params any;

  tmp := case when (DB.DBA.IMAP__paramGet (colId, 'C', 'connection', 0) = 'ssl') then 1 else 0 end;
  params := vector (
              'connection', tmp,
              'server',   DB.DBA.IMAP__paramGet (colId, 'C', 'server', 0),
              'port',     DB.DBA.IMAP__paramGet (colId, 'C', 'port', 0),
              'user',     DB.DBA.IMAP__paramGet (colId, 'C', 'user', 0),
              'password', DB.DBA.IMAP__paramGet (colId, 'C', 'password', 0),
              'folder',   DB.DBA.IMAP__paramGet (colId, 'C', 'folder', 0),
              'graph',    DB.DBA.IMAP__paramGet (colId, 'C', 'graph', 0),
              'sponger',  DB.DBA.IMAP__paramGet (colId, 'C', 'sponger', 0)
            );

  return params;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__paramGet (
  in id any,
  in what varchar,
  in propName varchar,
  in serialized integer := 1,
  in prefixed integer := 1,
  in decrypt integer := 0)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__paramGet (', id, what, propName, serialized, ')');
  declare propValue any;

  if (prefixed)
    propName := sprintf ('virt:%s-%s', DB.DBA.IMAP__detName (), propName);

  propValue := DB.DBA.DAV_PROP_GET_INT (DB.DBA.IMAP__davId (id), what, propName, 0, DB.DBA.IMAP__user (http_dav_uid ()), DB.DBA.IMAP__password (http_dav_uid ()), http_dav_uid ());
  if (isinteger (propValue))
    propValue := null;

  if (serialized and not isnull (propValue))
    propValue := deserialize (propValue);

  if (decrypt and not isnull (propValue))
    propValue := pwd_magic_calc ('imap', propValue, 1);

  return propValue;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__paramSet (
  in id any,
  in what varchar,
  in propName varchar,
  in propValue any,
  in serialized integer := 1,
  in prefixed integer := 1,
  in encrypt integer := 0)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__paramSet', _propName, _propValue, ')');
  declare retValue any;

  if (serialized)
    propValue := serialize (propValue);

  if (encrypt)
    propValue := pwd_magic_calc ('imap', propValue);

  if (prefixed)
    propName := sprintf ('virt:%s-%s', DB.DBA.IMAP__detName (), propName);

  id := DB.DBA.IMAP__davId (id);
  retValue := DB.DBA.DAV_PROP_SET_RAW (id, what, propName, propValue, 1, http_dav_uid ());

  return retValue;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__paramRemove (
  in id any,
  in what varchar,
  in propName varchar,
  in prefixed integer := 1)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__paramRemove (', _id, _what, _propName, ')');
  if (prefixed)
    propName := sprintf ('virt:%s-%s', DB.DBA.IMAP__detName (), propName);

  return DB.DBA.DAV_PROP_REMOVE_RAW (DB.DBA.IMAP__davId (id), what, propName, 1, http_dav_uid());
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderParamSet (
  in owner varchar,
  in id integer,
  in paramName varchar,
  in paramValue any)
{
  declare N integer;
  declare params any;

  if (id = 0)
  {
    id := cast (replace (owner, 'IMAP_', '') as integer);
    DB.DBA.IMAP__paramSet (id, 'C', paramName, paramValue);
    return;
  }
  params := (SELECT deserialize (MF_PARAMS) from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_ID = id);
  if (isnull (params))
    params := vector ();

  N := position (paramName, params);
  if (N)
  {
    params[N] := paramValue;
  } else {
    params := vector_concat (params, vector (paramName, paramValue));
  }
  update DB.DBA.MAIL_FOLDER
     set MF_PARAMS = serialize (params)
   where MF_OWN = owner and MF_ID = id;

  commit work;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderParamGet (
  in owner varchar,
  in id integer,
  in paramName varchar)
{
  declare params, paramValue any;

  if (id)
  {
    params := (SELECT deserialize (MF_PARAMS) from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_ID = id);
    if (not isnull (params))
      return get_keyword (paramName, params);

    return null;
  }
  id := cast (replace (owner, 'IMAP_', '') as integer);
  paramValue := DB.DBA.IMAP__paramGet (id, 'C', paramName, 1);

  return paramValue;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__selectParamGet (
  in _param varchar,
  in _data varchar,
  in _defaultValue varchar)
{
  declare pos, value, tmp any;

  value := _defaultValue;
  pos := strstr (_data, _param);
  if (pos is not NULL)
  {
    tmp := subseq (_data, pos+length (_param)+1);
    if (tmp[0] = ascii ('"'))
    {
      tmp := subseq (tmp, 1);
      pos := strstr (tmp, '"');
    }
    else if (tmp[0] = ascii ('('))
    {
      tmp := subseq (tmp, 1);
      pos := strstr (tmp, ')');
    }
    else
    {
      pos := strstr (tmp, ' ');
    }
    if (pos is not NULL)
      value := subseq (tmp, 0, pos);
  }
  return value;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__verify (
  in _connect_type varchar,
  in _server varchar,
  in _port varchar,
  in _userName varchar,
  in _userPassword varchar)
{
  declare _error, _connectType any;
  declare exit handler for sqlstate '*'
  {
    _error := __SQL_MESSAGE;
    _error := substring (_error, 1, coalesce (strstr (_error, '<>'), length (_error)));
    _error := substring (_error, 1, coalesce (strstr (_error, '\nin'), length (_error)));
    if ((length (_error) > 5) and (_error[5] = ascii (':')))
      _error := subseq (_error, 7);

    goto _exit;
  };

  _error       := '';
  _connectType := case when _connect_type = 'ssl' then 1 else 0 end;
  imap_get (sprintf ('%s:%s', _server, _port), _userName, _userPassword, '', '', null, _connectType);

_exit:;
  return _error;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__exec (
  in detcol_id integer,
  in params any,
  in param4 varchar := '',
  in param5 varchar := null,
  in param6 varchar := null)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__exec (', detcol_id, params, param4, param5, param6, ')');
  declare retValue any;
  declare exit handler for sqlstate '*'
  {
    DB.DBA.IMAP__activity (detcol_id, sprintf ('IMAP error %s: ', __SQL_STATE) || __SQL_MESSAGE);
    return null;
  };

  commit work;
  retValue := imap_get (sprintf ('%s:%s', get_keyword ('server', params), get_keyword ('port', params, '143')), get_keyword ('user', params), get_keyword ('password', params), param4, param5, param6, get_keyword ('connection', params));

  return retValue;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__user (
  in user_id integer,
  in default_id integer := null)
{
  return coalesce ((select U_NAME from DB.DBA.SYS_USERS where U_ID = coalesce (user_id, default_id)), '');
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__password (
  in user_id integer)
{
  return coalesce ((select pwd_magic_calc (U_NAME, U_PWD, 1) from WS.WS.SYS_DAV_USER where U_ID = user_id), '');
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__owner (
  in colId integer)
{
  colId := DB.DBA.IMAP__davId (colId);
  return sprintf ('IMAP_%d', colId);

}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__davId (
  in col_id any)
{
  if (isinteger (col_id))
    return col_id;

  return col_id[1];
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__path (
  in owner varchar,
  in id integer,
  in what varchar)
{
  declare detcol_id integer;
  declare path varchar;

  detcol_id := cast (replace (owner, 'IMAP_', '') as integer);
  path := DB.DBA.DAV_SEARCH_PATH (detcol_id, 'C');

  if (what = 'C')
    return rtrim (path || DB.DBA.IMAP__folderPath (owner, id), '/') || '/';

  if (what = 'R')
    return path || DB.DBA.IMAP__folderPath (owner, DB.DBA.IMAP__messageFolderId (owner, id)) || '/' || DB.DBA.IMAP__nameCompose (id, DB.DBA.IMAP__messageSubject (owner, id));

  return null;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__dateFromRFC822 (
  in _dt varchar,
  in _quiet integer := 1)
{
  declare N, pos integer;
  declare d, m, y, hh, mm, ss, zz varchar;
  declare zh, zm integer;
  declare V any;
  declare retValue datetime;

  pos := strstr (_dt, ',');
  if (not isnull (pos))
    _dt := subseq (_dt, pos+2);

  -- date
  N := 0;
  V := regexp_parse ('^(0[1-9]|[12][0-9]|3[01])[\\s-.](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\\s-.]((19|20)[0-9][0-9])', _dt, N);
  if (isnull (V))
    goto _error;

  d := subseq (_dt, V[2], V[3]);
  m := get_keyword (ucase (subseq (_dt, V[4], V[5])), vector ('JAN', '01', 'FEB', '02', 'MAR', '03', 'APR', '04', 'MAY', '05', 'JUN', '06', 'JUL', '07', 'AUG', '08', 'SEP', '09', 'OCT', '10', 'NOV', '11', 'DEC', '12'), '00');
  y := subseq (_dt, V[6], V[7]);

  -- time
  N := V[1];
  V := regexp_parse ('([01][0-9]|[2][0-3]):([0-5][0-9]):([0-9][0-9])', _dt, N);
  if (isnull (V))
    goto _error;

  hh := subseq (_dt, V[2], V[3]);
  mm := subseq (_dt, V[4], V[5]);
  ss := subseq (_dt, V[6], V[7]);

  -- timezone
  N := V[1];
  V := regexp_parse ('([+-])([0-9][0-9])([0-9][0-9])', _dt, N);
  if (isnull (V))
    goto _error;

  zz := subseq (_dt, V[2], V[3]);
  zh := atoi (subseq (_dt, V[4], V[5]));
  zm := atoi (subseq (_dt, V[6], V[7]));
  if (zz = '+')
  {
    zh := -zh;
    zm := -zm;
  }
  retValue := stringdate (sprintf ('%s.%s.%sT%s:%s:%s', y, m, d, hh, mm, ss));
  retValue := dateadd ('minute', timezone (curdatetime_tz()), retValue);
  retValue := dateadd ('hour',   zh, retValue);
  retValue := dateadd ('minute', zm, retValue);

  return retValue;

_error:;
  return stringdate ('1900.01.01 00:00:00');
}
;

-----------------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__dateConvert (
  in pString varchar,
  in pDefault any := null)
{
  declare continue handler for sqlstate '*' { goto _1; };
  return DB.DBA.IMAP__dateFromRFC822 (pString, 0);

_1:
  declare continue handler for sqlstate '*' { goto _2; };
  return http_string_date (pString);

_2:
  declare continue handler for sqlstate '*' { goto _end; };
  return stringdate (pString);

_end:
  return pDefault;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderPath (
  in owner varchar,
  in id integer)
{
  declare exit handler for not found { goto _0; };
  declare path, name varchar;
  declare step, parentId integer;

  step := 0;
  path := '/';
  while (id > 0)
  {
    step := step + 1;
    if ((step > 4) and (__proc_exists ('MAIL.WA.folder_circles_check') is not null) and MAIL.WA.folder_circles_check (owner, id))
      goto _0;

    select MF_NAME, MF_PARENT_ID into name, parentId from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_ID = id;
    id := parentId;
    path := concat ('/', name, path);
  }
  return trim (path, '/');

_0:
  return null;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderImapPath (
  in owner varchar,
  in id integer)
{
  return coalesce (DB.DBA.IMAP__folderParamGet (owner, id, 'imapPath'), '');
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderImapDelimiter (
  in owner varchar,
  in id integer)
{
  return coalesce (DB.DBA.IMAP__folderParamGet (owner, id, 'imapDelimiter'), '.');
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderId_work (
  in owner varchar,
  in path_parts any,
  in mode integer)
{
  declare N integer;
  declare id, parentId integer;

  id := null;
  parentId := 0;
  for (N := 0; N < length (path_parts) - mode; N := N + 1)
  {
    id := (select MF_ID from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_PARENT_ID = parentId and MF_NAME = path_parts[N]);
    if (isnull (id))
      goto _0;

    parentId := id;
  }
_0:
  return id;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderId (
  in owner varchar,
  in path_parts any)
{
  return DB.DBA.IMAP__folderId_work (owner, path_parts, 1);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderParentId (
  in owner varchar,
  in path_parts any)
{
  return DB.DBA.IMAP__folderId_work (owner, path_parts, 2);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderName (
  in owner varchar,
  in id integer)
{
  return (select MF_NAME from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_ID = id);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderCreate (
  in owner varchar,
  in parentId integer,
  in name varchar,
  in imapPath varchar,
  in imapDelimiter varchar := '.',
  in imapSelect varchar := null)
{
  declare id integer;

  id := (SELECT MF_ID from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_PARENT_ID = parentId and MF_NAME = name);
  if (isnull (id))
  {
    set isolation = 'serializable';
    id := coalesce ((select max (MF_ID) from MAIL_FOLDER where MF_OWN = owner), 0) + 1;
    insert into DB.DBA.MAIL_FOLDER (MF_ID, MF_PARENT_ID, MF_OWN, MF_NAME, MF_IS_READED, MF_IS_ANSWERED, MF_IS_FLAGGED, MF_IS_DELETED, MF_IS_DRAFT, MF_IS_NOINFERIORS, MF_IS_NOSELECT, MF_IS_MARKED, MF_IS_UNMARKED, MF_IS_ACTIVE)
      values (id, parentId, owner, name, 2, 1, 1, 2, 1,  0, 0, 1, 0, 1);
  }
  DB.DBA.IMAP__folderParamSet (owner, id, 'imapPath', imapPath);
  DB.DBA.IMAP__folderParamSet (owner, id, 'imapDelimiter', imapDelimiter);
  if (not isnull (imapSelect))
    DB.DBA.IMAP__folderParamSet (owner, id, 'imapSelect', imapSelect);

  return id;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderErase (
  in detcol_id integer,
  in owner varchar,
  in folder_id integer,
  in params any)
{
  declare step integer;
  declare exit handler for sqlstate 'LOOP'
  {
    return -1;
  };

  step := 0;
  return DB.DBA.IMAP__folderErase_internal (detcol_id, owner, folder_id, params, step);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderErase_internal (
  in detcol_id integer,
  in owner varchar,
  in folder_id integer,
  in params any,
  inout step integer)
{
  for (select MF_ID from DB.DBA.MAIL_FOLDER where MF_PARENT_ID = folder_id) do
  {
    step := step + 1;
    if ((step > 4) and (__proc_exists ('MAIL.WA.folder_circles_check') is not null) and MAIL.WA.folder_circles_check (owner, folder_id))
      signal ('LOOP', 'Bad path');

    DB.DBA.IMAP__folderErase_internal (detcol_id, owner, MF_ID, params, step);
  }

  DB.DBA.IMAP__messagesErase (detcol_id, owner, folder_id, params);
  delete from DB.DBA.MAIL_FOLDER where MF_ID = folder_id;

  return 1;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__davList (
  in detcol_id any,
  in owner varchar,
  in params any,
  in path_parts any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__davList (', detcol_id, owner, path_parts, ')');
  declare folderId integer;
  declare retValue, detEntry any;

  vectorbld_init (retValue);
  detEntry := DB.DBA.DAV_DIR_SINGLE_INT (detcol_id, 'C', '', null, null, http_dav_uid ());
  folderId := coalesce (DB.DBA.IMAP__folderId (owner, path_parts), 0);
  for (select * from DB.DBA.MAIL_FOLDER where MF_OWN = owner and MF_PARENT_ID = folderId and MF_IS_DELETED <> 1) do
  {
    vectorbld_acc (
      retValue,
      vector (
        detEntry[0] || DB.DBA.IMAP__folderPath (owner, MF_ID) || '/',
        'C',
        0,
        detEntry[3],
        vector (UNAME'IMAP', detcol_id, MF_ID, 'C'),
        detEntry[5],
        detEntry[6],
        detEntry[7],
        detEntry[8],
        'dav/unix-directory',
        MF_NAME
      )
    );
  }
  for (select * from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_PARENT_ID = 0 and MM_FLD_ID = folderId and MM_IS_DELETED <> 1) do
  {
    vectorbld_acc (
      retValue,
      vector (
        detEntry[0] || DB.DBA.IMAP__folderPath (owner, folderId) || '/' || DB.DBA.IMAP__nameCompose (MM_ID, MM_SUBJ),
        'R',
        length (MM_BODY),
        DB.DBA.IMAP__dateConvert (MM_SND_TIME),
        vector (UNAME'IMAP', detcol_id, MM_ID, 'R'),
        detEntry[5],
        detEntry[6],
        detEntry[7],
        DB.DBA.IMAP__dateConvert (MM_SND_TIME),
        'message/rfc822',
        DB.DBA.IMAP__nameCompose (MM_ID, MM_SUBJ)
      )
    );
  }
  vectorbld_final (retValue);
  return retValue;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__load (
  in detcol_id any,
  in owner varchar,
  in params any,
  in path_parts any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__load (', detcol_id, owner, path_parts, ')');
  declare folderId integer;

  folderId := coalesce (DB.DBA.IMAP__folderId (owner, path_parts), 0);
  DB.DBA.DAV_QUEUE_ADD (owner, folderId, 'DB.DBA.IMAP__load_aq', vector (detcol_id, owner, params, path_parts), 2, 0);
  DB.DBA.DAV_QUEUE_INIT ();
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__load_aq (
  in queue_id integer,
  in detcol_id any,
  in owner varchar,
  in params any,
  in path_parts any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__load_aq (', detcol_id, queue_id, owner, path_parts, ')');
  DB.DBA.IMAP__folderIMAPList (queue_id, detcol_id, owner, params, path_parts);
  DB.DBA.IMAP__folderIMAPSelect (queue_id, detcol_id, owner, params, path_parts);

  DB.DBA.DAV_QUEUE_UPDATE_STATE (queue_id, 2);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderIMAPList (
  in queue_id integer,
  in detcol_id any,
  in owner varchar,
  in params any,
  in path_parts any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__folderIMAPList (', queue_id, detcol_id, owner, params, path_parts, ')');
  declare N, M, folderId, folderParentId integer;
  declare tmp, imapPath, imapDelimiter, pathList varchar;
  declare listLoadTime datetime;
  declare checked, checkInterval, folders, folderTemplate, folderTemplateParts, folderPath, folderParts, folderFlags, folderSelect, folderDelimiter, folderName any;

  listLoadTime := DB.DBA.IMAP__paramGet (detcol_id, 'C', 'listLoadTime');
  if (not isnull (listLoadTime))
  {
    checkInterval := DB.DBA.IMAP__paramGet (detcol_id, 'C', 'checkInterval', 0);
    if (isinteger (checkInterval) or isnull (checkInterval))
      checkInterval := '15';

    checkInterval := atoi (checkInterval) * 60;
    if (datediff ('second', listLoadTime, now ()) < checkInterval)
      return;
  }

  folderTemplate := get_keyword ('folder', params, '');
  imapPath := DB.DBA.IMAP__folderImapPath (owner, folderId);
  imapDelimiter := DB.DBA.IMAP__folderImapDelimiter (owner, folderId);
  pathList := case when imapPath = '' then '*' else imapPath || imapDelimiter || '*' end;

  -- activity
  DB.DBA.IMAP__activity (detcol_id, sprintf ('Folder''s list (%s) update is started', case when imapPath = '' then '[Root]' else imapPath end));

  folders := DB.DBA.IMAP__exec (detcol_id, params, 'list', pathList);
  checked := vector ();
  if (length (folders))
  {
    foreach (any folder in folders) do
    {
      folderParts := regexp_parse('\\((.*)\\)\\s\\"(.*)\\"\\s\\"?(.*)((\\r\\n)|\\")', folder, 0);
      if (length (folderParts) > 7)
      {
        folderParentId := 0;
        folderFlags := subseq (folder, folderParts[2], folderParts[3]);
        folderDelimiter := subseq (folder, folderParts[4], folderParts[5]);
        folderName := subseq (folder, folderParts[6], folderParts[7]);
        folderName := rtrim (folderName, '\n');
        folderName := rtrim (folderName, '\r');
        folderName := rtrim (folderName, '"');
        folderTemplate := replace (folderTemplate, '/', folderDelimiter);
        if ((pathList = '*') and (folderTemplate <> '') and not starts_with (folderName, folderTemplate))
          goto _skip;

        folderTemplateParts := split_and_decode (folderTemplate, 0, '\0\0' || folderDelimiter);
        folderParts := split_and_decode (folderName, 0, '\0\0' || folderDelimiter);
        for (N := 0; N < length (folderParts); N := N + 1)
        {
          folderPath := '';
          for (M := 0; M <= N; M := M + 1)
            folderPath := folderPath || folderDelimiter || folderParts[M];
          folderPath := trim (folderPath, folderDelimiter);

          if (N >= length (folderTemplateParts)-1)
          {
            folderSelect := null;
            if ((folderName = folderPath) and (not isnull (strstr (lcase (folderFlags), '\\noselect'))))
              folderSelect := 'no';

            folderParentId := DB.DBA.IMAP__folderCreate (owner, folderParentId, folderParts[N], trim (folderPath, folderDelimiter), folderDelimiter, folderSelect);
          }
          commit work;
          DB.DBA.DAV_QUEUE_UPDATE_TS (queue_id);
        }
        checked := vector_concat (checked, vector (folderName));
      _skip:;
      }
    }
  }
  for (select MF_ID from DB.DBA.MAIL_FOLDER where MF_OWN = owner) do
  {
    tmp := DB.DBA.IMAP__folderImapPath (owner, MF_ID);
    if ((tmp = '') or ((strstr (tmp, imapPath) = 0) and (tmp <> imapPath)))
    {
      if ((tmp = '') or not position (tmp, checked))
        DB.DBA.IMAP__folderErase (detcol_id, owner, MF_ID, params);
    }
  }
  DB.DBA.IMAP__folderParamSet (owner, folderId, 'listLoadTime', now ());

  -- activity
  DB.DBA.IMAP__activity (detcol_id, sprintf ('Folder''s list (%s) update is ended', case when imapPath = '' then '[Root]' else imapPath end));
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderIMAPSelect (
  in queue_id integer,
  in detcol_id integer,
  in owner varchar,
  in params any,
  in path_parts any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__folderIMAPSelect (', queue_id, detcol_id, owner, path_parts, ')');
  declare _count, folderId, _isNotDeleted integer;
  declare imapPath, imapSelect varchar;
  declare selectLoadTime datetime;
  declare messages, messageUIDs, fetchMessage, checkInterval any;
  declare _id, _flags, _msgId any;

  folderId := coalesce (DB.DBA.IMAP__folderId (owner, path_parts), 0);
  if (folderId = 0)
    return;

  selectLoadTime := DB.DBA.IMAP__folderParamGet (owner, folderId, 'selectLoadTime');
  if (not isnull (selectLoadTime))
  {
    checkInterval := DB.DBA.IMAP__paramGet (detcol_id, 'C', 'checkInterval', 0);
    if (isinteger (checkInterval) or isnull (checkInterval))
      checkInterval := '15';

    checkInterval := atoi (checkInterval) * 60;
    if (datediff ('second', selectLoadTime, now ()) < checkInterval)
      return;
  }

  imapSelect := DB.DBA.IMAP__folderParamGet (owner, folderId, 'imapSelect');
  if (imapSelect = 'no')
    return;

  imapPath := DB.DBA.IMAP__folderImapPath (owner, folderId);

  -- activity
  _count := 0;
  DB.DBA.IMAP__activity (detcol_id, sprintf ('Folder''s content (%s) update is started', imapPath));

  messages := DB.DBA.IMAP__exec (detcol_id, params, 'select', imapPath);
  vectorbld_init (messageUIDs);
  foreach (any message in messages) do
  {
    _flags := DB.DBA.IMAP__selectParamGet ('FLAGS', message[1], '');
    _msgId := DB.DBA.IMAP__selectParamGet ('UID', message[1], message[0]);
    _isNotDeleted := case when isnull (strstr (_flags, '\\deleted')) then 1 else 0 end;
    _id := (select MM_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_FLD_ID = folderId and MM_MEA_MSG_ID = _msgId);
    if (_isNotDeleted)
    {
      if (isnull (_id))
      {
      _fetch:;
        fetchMessage := DB.DBA.IMAP__exec (detcol_id, params, 'fetch', imapPath, vector (cast (_msgId as integer)));
        if (length (fetchMessage))
        {
          _id := (select MM_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_FLD_ID = folderId and MM_MEA_MSG_ID = _msgId);
          if (isnull (_id))
          {
            _id := DB.DBA.IMAP__messageCreate (detcol_id, owner, _msgId, params, folderId, fetchMessage[0][1]);
            if (_id > 0)
              _count := _count + 1;
          }
        }
      }
      vectorbld_acc (messageUIDs, cast (_msgId as varchar));
    }
    else if (not isnull (_id))
    {
      DB.DBA.IMAP__messageErase (detcol_id, owner, _id, params);
    }
    commit work;
    DB.DBA.DAV_QUEUE_UPDATE_TS (queue_id);
  }
  vectorbld_final (messageUIDs);
  for (select MM_ID, MM_MEA_MSG_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_FLD_ID = folderId) do
  {
    if (not position (MM_MEA_MSG_ID, messageUIDs))
      DB.DBA.IMAP__messageErase (detcol_id, owner, MM_ID, params);
  }
  DB.DBA.IMAP__folderParamSet (owner, folderId, 'selectLoadTime', now ());

  -- activity
  DB.DBA.IMAP__activity (detcol_id, sprintf ('Folder''s content (%s) update is ended (%d new messages)', imapPath, _count));
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderIMAPCreate (
  in detcol_id integer,
  in owner varchar,
  in params any,
  in path_parts any)
{
  declare folderId, folderParentId integer;
  declare imapPath, imapDelimiter varchar;

  folderParentId := coalesce (DB.DBA.IMAP__folderParentId (owner, path_parts), 0);
  if (folderParentId = 0)
    return -1;

  imapPath := DB.DBA.IMAP__folderImapPath (owner, folderParentId);
  imapDelimiter := DB.DBA.IMAP__folderImapDelimiter (owner, folderParentId);
  imapPath := imapPath || imapDelimiter || path_parts[length(path_parts)-2];

  DB.DBA.IMAP__exec (detcol_id, params, 'create', imapPath);

  return DB.DBA.IMAP__folderCreate (owner, folderParentId, path_parts[length(path_parts)-2], imapPath, imapDelimiter);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderIMAPErase (
  in detcol_id integer,
  in owner varchar,
  in params any,
  in path_parts any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__folderIMAPErase (', detcol_id, owner, path_parts, ')');
  declare folder_id integer;

  folder_id := coalesce (DB.DBA.IMAP__folderId (owner, path_parts), 0);
  if (folder_id = 0)
    return -1;

  update DB.DBA.MAIL_FOLDER
     set MF_IS_DELETED = 1
   where MF_OWN = owner
     and MF_ID = folder_id;

  DB.DBA.DAV_QUEUE_ADD ('IMAP', folder_id, 'DB.DBA.IMAP__folderIMAPErase_aq', vector (detcol_id, owner, params, path_parts, connection_get ('dav_store')), 0, 0);
  DB.DBA.DAV_QUEUE_INIT ();

  return 1;
}
;


-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__folderIMAPErase_aq (
  in queue_id integer,
  in detcol_id integer,
  in owner varchar,
  in params any,
  in path_parts any,
  in save any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__folderIMAPErase_aq (', queue_id, detcol_id, owner, path_parts, save, ')');
  declare folder_id integer;
  declare imapPath, imapDelimiter varchar;
  declare retValue any;

  folder_id := coalesce (DB.DBA.IMAP__folderId (owner, path_parts), 0);
  if (folder_id = 0)
    return -1;

  if (save is null)
  {
    imapPath := DB.DBA.IMAP__folderImapPath (owner, folder_id);
    DB.DBA.IMAP__exec (detcol_id, params, 'delete', imapPath);
  }
  retValue := DB.DBA.IMAP__folderErase (detcol_id, owner, folder_id, params);
  DB.DBA.DAV_QUEUE_UPDATE_STATE (queue_id, 2);

  return retValue;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__nameFix (
  in name varchar)
{
  return replace (replace (replace (replace (name, '/', '_'), '\\', '_'), ':', '_'), '?', '_');;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__nameCompose (
  in msg_id integer,
  in subject varchar)
{
  return sprintf ('%s - M''%d.eml', DB.DBA.IMAP__nameFix (subject), msg_id);
}
;

create function DB.DBA.IMAP__msgId (
  in _name varchar)
{
  declare V any;

  V := regexp_parse ('M''([0-9]*).eml', _name, 0);
  if (length (V) < 4)
    return -1;

  return cast (subseq (_name, V[2], V[3]) as integer);
}
;

create function DB.DBA.IMAP__messageId (
  in owner varchar,
  in msg_id integer)
{
  return cast ((select MM_MEA_MSG_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = msg_id) as integer);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messageFolderId (
  in owner varchar,
  in msg_id integer)
{
  return coalesce ((select MM_FLD_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = msg_id), 0);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messageSubject (
  in owner varchar,
  in msg_id integer)
{
  return coalesce ((select MM_SUBJ from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = msg_id), '');
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messageContent (
  in owner varchar,
  in msg_id integer)
{
  return (select MM_BODY from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_ID = msg_id);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messageCreate (
  in detcol_id integer,
  in owner varchar,
  in msgId varchar,
  in params any,
  in folderId integer,
  in message any)
{
  declare id integer;

  if (__proc_exists ('MAIL.WA.message_receive') is not null)
  {
    id := MAIL.WA.message_receive (owner, 0, message, 0, null, msgId, folderId);
  }
  else if (not exists (select 1 from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_FLD_ID = folderId and MM_MEA_MSG_ID = msgId))
  {
    declare _subj, _cc, _bcc, _sent, _to, _from, _mid, _msg varchar;

    _msg  := blob_to_string (message);
    _subj := substring (mail_header (_msg, 'Subject'), 1, 512);
    _cc   := substring (mail_header (_msg, 'Cc'), 1, 512);
    _bcc  := substring (mail_header (_msg, 'Bcc'), 1, 512);
    _sent := substring (mail_header (_msg, 'Date'), 1, 50);
    _to   := substring (mail_header (_msg, 'To'), 1, 512);
    _from := substring (mail_header (_msg, 'From'), 1, 512);
    _mid  := substring (mail_header (_msg, 'Message-ID'), 1, 512);

    set isolation = 'serializable';
    id := coalesce ((select max (MM_ID) from DB.DBA.MAIL_MESSAGE where MM_OWN = owner), 0) + 1;
    insert into DB.DBA.MAIL_MESSAGE (MM_ID, MM_OWN, MM_FLD_ID, MM_FLD, MM_BODY, MM_REC_DATE, MM_SUBJ, MM_CC, MM_BCC, MM_SND_TIME, MM_TO, MM_FROM, MM_MSG_ID, MM_IS_FETCHED)
      values (id, owner, folderId, DB.DBA.IMAP__folderImapPath (owner, folderId), _msg, cast (now () as varchar), _subj, _cc, _bcc, _sent, _to, _from, msgId, 1);
    set isolation = 'committed';
  }
  DB.DBA.IMAP__message_sioc_insert (detcol_id, owner, id, params);

  return id;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messageMove (
  in owner varchar,
  in messageId integer,
  in folderId integer,
  in mode integer := 0)
{
  update DB.DBA.MAIL_MESSAGE
     set MM_FLD_ID = folderId,
         MM_FLD = DB.DBA.IMAP__folderImapPath (owner, folderId)
   where MM_OWN = owner
     and MM_MSG_ID = messageId;

  if (not mode)
    return 1;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messageErase (
  in detcol_id integer,
  in owner varchar,
  in id integer,
  in params any)
{
  delete
    from DB.DBA.MAIL_ATTACHMENT
   where MA_M_OWN = owner and MA_M_ID = id;

  delete
    from DB.DBA.MAIL_MESSAGE
   where MM_OWN = owner and MM_ID = id;

  DB.DBA.IMAP__message_sioc_delete (detcol_id, owner, id, params);
  commit work;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messagesErase (
  in detcol_id integer,
  in owner varchar,
  in folder_id integer,
  in params any)
{
  for (select MM_ID from DB.DBA.MAIL_MESSAGE where MM_OWN = owner and MM_FLD_ID = folder_id) do
  {
    DB.DBA.IMAP__messageErase (detcol_id, owner, MM_ID, params);
  }
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__messageIMAPErase (
  in detcol_id integer,
  in owner varchar,
  in params any,
  in path_parts any)
{
  declare msg_id, folder_id integer;
  declare imapPath varchar;
  declare save any;

  msg_id := DB.DBA.IMAP__msgId (path_parts [length (path_parts)-1]);
  save := connection_get ('dav_store');
  if (save is null)
  {
    folder_id := DB.DBA.IMAP__messageFolderId (owner, msg_id);
    if (folder_id = 0)
      return -1;

    imapPath := DB.DBA.IMAP__folderImapPath (owner, folder_id);
    DB.DBA.IMAP__exec (detcol_id, params, 'message_delete', imapPath, vector (DB.DBA.IMAP__messageId (owner, msg_id)));
  }
  DB.DBA.IMAP__messageErase (detcol_id, owner, msg_id, params);

  return 1;
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__activity (
  in detcol_id integer,
  in text varchar)
{
  DB.DBA.DAV_DET_ACTIVITY (DB.DBA.IMAP__detName (), detcol_id, text);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__refresh (
  in path varchar)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__refresh (', path, ')');
  declare id, owner any;

  id := DB.DBA.DAV_SEARCH_ID (path, 'C');
  owner := DB.DBA.IMAP__owner (id);
  if (isinteger (id))
  {
    DB.DBA.IMAP__paramRemove (id, 'C', 'listLoadTime');
    DB.DBA.IMAP__paramRemove (id, 'C', 'selectLoadTime');
  }
  else
  {
    DB.DBA.IMAP__folderParamSet (owner, id[2], 'listLoadTime', null);
    DB.DBA.IMAP__folderParamSet (owner, id[2], 'selectLoadTime', null);
  }
}
;

-------------------------------------------------------------------------------
--
create procedure DB.DBA.IMAP__WAC_INSERT (
  in owner varchar,
  in id integer,
  in what varchar,
  in content varchar)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__WAC_INSERT (', owner, id, what, ')');
  declare path, graph varchar;

  path := DB.DBA.IMAP__path (owner, id, what);
  graph := WS.WS.WAC_GRAPH (path);
  DB.DBA.TTLP (content, graph, graph);
}
;

-------------------------------------------------------------------------------
--
create procedure DB.DBA.IMAP__WAC_DELETE (
  in owner varchar,
  in id integer,
  in what varchar)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__WAC_DELETE (', owner, id, what, ')');
  declare path, graph varchar;

  path := DB.DBA.IMAP__path (owner, id, what);
  graph := WS.WS.WAC_GRAPH (path);
  set_user_id ('dba');
  delete from DB.DBA.RDF_QUAD where G = iri_to_id (graph);
}
;

-------------------------------------------------------------------------------
--
create procedure DB.DBA.IMAP__message_sioc_insert (
  in detcol_id integer,
  in owner varchar,
  in msgId integer,
  in params any,
  in rdf_params any := null)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__message_sioc_insert (', ')');
  declare rc, rc2 integer;
  declare colId, creatorId integer;
  declare iri, graph_iri, graph2_iri, privateGroup, creator_iri, sponger varchar;
  declare content, type, cartridges, metaCartridges any;
  declare exit handler for sqlstate '*'
  {
    SIOC..sioc_log_message (__SQL_MESSAGE);
    return;
  };

  if (isnull (VAD_CHECK_VERSION ('Mail')))
    return;

  if (isnull (rdf_params))
    rdf_params := DB.DBA.DAV_DET_RDF_PARAMS_GET (DB.DBA.IMAP__detName (), detcol_id);

  graph_iri := get_keyword ('graph', rdf_params, '');
  if (graph_iri = '')
    return;

  colId := atoi (replace (owner, 'IMAP_', ''));
  sponger :=  get_keyword ('sponger', rdf_params, 'off');
  if (DB.DBA.is_empty_or_null (sponger) or (sponger = 'off'))
  {
    creatorId := coalesce ((select COL_OWNER from WS.WS.SYS_DAV_COL where COL_ID = colId), -1);
    creator_iri := SIOC..user_iri (creatorId);
    SIOC..message_insert_internal (graph_iri, null, creator_iri, owner, msgId);
  }
  else
  {
    iri := SIOC..mail_post_iri (owner, msgId);
    graph2_iri := iri || '.eml';

    content := cast (DB.DBA.IMAP__messageContent (owner, msgId) as varchar);
    type := 'message/rfc822';
    cartridges := get_keyword ('cartridges', rdf_params);
    metaCartridges := get_keyword ('metaCartridges', rdf_params);
    rc := DB.DBA.RDF_SINK_UPLOAD_CARTRIDGES (content, type, 'select RM_ID, RM_PATTERN, RM_TYPE, RM_HOOK, RM_KEY, RM_OPTIONS from DB.DBA.SYS_RDF_MAPPERS where RM_ENABLED = 1 order by RM_ID', iri, graph2_iri, cartridges);
    rc2 := DB.DBA.RDF_SINK_UPLOAD_CARTRIDGES (content, type, 'select MC_ID, MC_PATTERN, MC_TYPE, MC_HOOK, MC_KEY, MC_OPTIONS from DB.DBA.RDF_META_CARTRIDGES where MC_ENABLED = 1 order by MC_SEQ, MC_ID', iri, graph2_iri, metaCartridges);
    if (rc or rc2)
    {
      declare exit handler for sqlstate '*' {
        SPARQL clear graph ?:graph2_iri;
        return;
      };
      SPARQL insert in graph ?:graph_iri { ?s ?p ?o } where { graph `iri(?:graph2_iri)` { ?s ?p ?o } };

      DB.DBA.DAV_DET_PRIVATE_GRAPH_ADD (graph2_iri);
    }
  }
}
;

-------------------------------------------------------------------------------
--
create procedure DB.DBA.IMAP__message_sioc_delete (
  in detcol_id integer,
  in owner varchar,
  in msgId integer,
  in params any,
  in rdf_params any := null)
{
  declare iri, graph_iri, graph2_iri, privateGroup, sponger varchar;
  declare exit handler for sqlstate '*'
  {
    SIOC..sioc_log_message (__SQL_MESSAGE);
    return;
  };

  if (isnull (VAD_CHECK_VERSION ('Mail')))
    return;

  if (isnull (rdf_params))
    rdf_params := DB.DBA.DAV_DET_RDF_PARAMS_GET (DB.DBA.IMAP__detName (), detcol_id);

  graph_iri := get_keyword ('graph', rdf_params, '');
  if (graph_iri = '')
    return;

  sponger :=  get_keyword ('sponger', rdf_params, 'off');
  if (DB.DBA.is_empty_or_null (sponger) or (sponger = 'off'))
  {
    SIOC..message_delete_internal (graph_iri, owner, msgId);
  }
  else
  {
    iri := SIOC..mail_post_iri (owner, msgId);
    graph2_iri := iri || '.eml';

    SPARQL delete from graph ?:graph_iri { ?s ?p ?o } where { graph `iri(?:graph2_iri)` { ?s ?p ?o } };
    SPARQL clear graph ?:graph2_iri;

    DB.DBA.DAV_DET_PRIVATE_GRAPH_REMOVE (graph2_iri);
  }
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__rdf_insert (
  in detcol_id integer,
  in id any,
  in what varchar,
  in rdf_params any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__rdf_insert (', detcol_id, id, what, rdf_params, ')');
  declare owner, permissions varchar;
  declare params any;

  owner := DB.DBA.IMAP__owner (detcol_id);
  params := DB.DBA.IMAP__params (detcol_id);
  return DB.DBA.IMAP__message_sioc_insert (detcol_id, owner, id[2], params, rdf_params);
}
;

-------------------------------------------------------------------------------
--
create function DB.DBA.IMAP__rdf_delete (
  in detcol_id integer,
  in id any,
  in what varchar,
  in rdf_params any)
{
  -- dbg_obj_princ ('DB.DBA.IMAP__rdf_delete (', detcol_id, id, what, rdf_params, ')');
  declare owner varchar;
  declare params any;

  owner := DB.DBA.IMAP__owner (detcol_id);
  params := DB.DBA.IMAP__params (detcol_id);
  return DB.DBA.IMAP__message_sioc_delete (detcol_id, owner, id[2], params, rdf_params);
}
;
