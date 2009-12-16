--
--  $Id$
--
--  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
--  project.
--
--  Copyright (C) 1998-2007 OpenLink Software
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
-------------------------------------------------------------------------------
--
-- Session Functions
--
-------------------------------------------------------------------------------
create procedure AB.WA.session_domain (
  inout params any)
{
  declare aPath, domain_id, options any;

  declare exit handler for sqlstate '*'
  {
    domain_id := -1;
    goto _end;
  };

  options := http_map_get('options');
  if (not is_empty_or_null (options))
    domain_id := get_keyword ('domain', options);
  if (is_empty_or_null (domain_id))
  {
    aPath := split_and_decode (trim (http_path (), '/'), 0, '\0\0/');
    domain_id := cast(aPath[1] as integer);
  }
  if (not exists (select 1 from DB.DBA.WA_INSTANCE where WAI_ID = domain_id))
    domain_id := -1;

_end:;
  return cast (domain_id as integer);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.session_restore(
  inout params any)
{
  declare aPath, domain_id, user_id, user_name, user_role, sid, realm, options any;

  declare exit handler for sqlstate '*'
  {
    domain_id := -2;
    goto _end;
  };

  sid := get_keyword('sid', params, '');
  realm := get_keyword('realm', params, '');

  options := http_map_get('options');
  if (not is_empty_or_null(options))
    domain_id := get_keyword('domain', options);
  if (is_empty_or_null (domain_id))
  {
    aPath := split_and_decode (trim (http_path (), '/'), 0, '\0\0/');
    domain_id := cast(aPath[1] as integer);
  }
  if (not exists(select 1 from DB.DBA.WA_INSTANCE where WAI_ID = domain_id and domain_id <> -2))
    domain_id := -1;

_end:
  domain_id := cast (domain_id as integer);
  user_id := -1;
  for (select U.U_ID,
              U.U_NAME,
              U.U_FULL_NAME
         from DB.DBA.VSPX_SESSION S,
              WS.WS.SYS_DAV_USER U
        where S.VS_REALM = realm
          and S.VS_SID   = sid
          and S.VS_UID   = U.U_NAME) do
  {
    user_id   := U_ID;
    user_name := AB.WA.user_name(U_NAME, U_FULL_NAME);
    user_role := AB.WA.access_role(domain_id, U_ID);
  }
  if ((user_id = -1) and (domain_id >= 0) and (not exists(select 1 from DB.DBA.WA_INSTANCE where WAI_ID = domain_id and WAI_IS_PUBLIC = 1)))
    domain_id := -1;

  if (user_id = -1)
    if (domain_id = -1)
    {
      user_role := 'expire';
      user_name := 'Expire session';
    } else if (domain_id = -2) {
      user_role := 'public';
      user_name := 'Public User';
    } else {
      user_role := 'guest';
      user_name := 'Guest User';
    }

  return vector('domain_id', domain_id,
                'user_id',   user_id,
                'user_name', user_name,
                'user_role', user_role
               );
}
;

-------------------------------------------------------------------------------
--
-- Freeze Functions
--
-------------------------------------------------------------------------------
create procedure AB.WA.frozen_check(in domain_id integer)
{
  declare exit handler for not found { return 1; };

  if (is_empty_or_null((select WAI_IS_FROZEN from DB.DBA.WA_INSTANCE where WAI_ID = domain_id)))
    return 0;

  declare user_id integer;

  user_id := (select U_ID from SYS_USERS where U_NAME = connection_get ('vspx_user'));
  if (AB.WA.check_admin(user_id))
    return 0;

  user_id := (select U_ID from SYS_USERS where U_NAME = connection_get ('owner_user'));
  if (AB.WA.check_admin(user_id))
    return 0;

  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.frozen_page(in domain_id integer)
{
  return (select WAI_FREEZE_REDIRECT from DB.DBA.WA_INSTANCE where WAI_ID = domain_id);
}
;

-------------------------------------------------------------------------------
--
-- User Functions
--
-------------------------------------------------------------------------------
create procedure AB.WA.check_admin(
  in user_id integer) returns integer
{
  declare group_id integer;
  group_id := (select U_GROUP from SYS_USERS where U_ID = user_id);

  if (user_id = 0)
    return 1;
  if (user_id = http_dav_uid ())
    return 1;
  if (group_id = 0)
    return 1;
  if (group_id = http_dav_uid ())
    return 1;
  if(group_id = http_dav_uid()+1)
    return 1;
  return 0;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.check_grants(in domain_id integer, in user_id integer, in role_name varchar)
{
  whenever not found goto _end;

  if (AB.WA.check_admin(user_id))
    return 1;
  if (role_name is null or role_name = '')
    return 0;
  if (role_name = 'admin')
    return 0;
  if (role_name = 'guest') {
    if (exists(select 1
                 from SYS_USERS A,
                      WA_MEMBER B,
                      WA_INSTANCE C
                where A.U_ID = user_id
                  and B.WAM_USER = A.U_ID
                  and B.WAM_INST = C.WAI_NAME
                  and C.WAI_ID = domain_id))
      return 1;
  }
  if (role_name = 'owner')
    if (exists(select 1
                 from SYS_USERS A,
                      WA_MEMBER B,
                      WA_INSTANCE C
                where A.U_ID = user_id
                  and B.WAM_USER = A.U_ID
                  and B.WAM_MEMBER_TYPE = 1
                  and B.WAM_INST = C.WAI_NAME
                  and C.WAI_ID = domain_id))
      return 1;
_end:
  return 0;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.check_grants2(in role_name varchar, in page_name varchar)
{
  declare tree any;

  tree := xml_tree_doc (AB.WA.menu_tree ());
  if (isnull (xpath_eval (sprintf ('//node[(@url = "%s") and contains(@allowed, "%s")]', page_name, role_name), tree, 1)))
    return 0;
  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.access_role(in domain_id integer, in user_id integer)
{
  whenever not found goto _end;

  if (AB.WA.check_admin (user_id))
    return 'admin';

  if (exists(select 1
               from SYS_USERS A,
                    WA_MEMBER B,
                    WA_INSTANCE C
              where A.U_ID = user_id
                and B.WAM_USER = A.U_ID
                and B.WAM_MEMBER_TYPE = 1
                and B.WAM_INST = C.WAI_NAME
                and C.WAI_ID = domain_id))
    return 'owner';

  if (exists(select 1
               from SYS_USERS A,
                    WA_MEMBER B,
                    WA_INSTANCE C
              where A.U_ID = user_id
                and B.WAM_USER = A.U_ID
                and B.WAM_MEMBER_TYPE = 2
                and B.WAM_INST = C.WAI_NAME
                and C.WAI_ID = domain_id))
    return 'author';

  if (exists(select 1
               from SYS_USERS A,
                    WA_MEMBER B,
                    WA_INSTANCE C
              where A.U_ID = user_id
                and B.WAM_USER = A.U_ID
                and B.WAM_INST = C.WAI_NAME
                and C.WAI_ID = domain_id))
    return 'reader';

  if (exists(select 1
               from SYS_USERS A
              where A.U_ID = user_id))
    return 'guest';

_end:
  return 'public';
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.access_is_write (in access_role varchar)
{
  if (is_empty_or_null (access_role))
    return 0;
  if (access_role = 'guest')
    return 0;
  if (access_role = 'public')
    return 0;
  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.wa_home_link ()
{
  return case when registry_get ('wa_home_link') = 0 then '/ods/' else registry_get ('wa_home_link') end;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.wa_home_title ()
{
  return case when registry_get ('wa_home_title') = 0 then 'ODS Home' else registry_get ('wa_home_title') end;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.page_name ()
{
  declare aPath any;

  aPath := http_path ();
  aPath := split_and_decode (aPath, 0, '\0\0/');
  return aPath [length (aPath) - 1];
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.menu_tree ()
{
  declare S varchar;

  S :=
'<?xml version="1.0" ?>
<menu_tree>
  <node name="home" url="home.vspx"            id="1"   allowed="public guest reader author owner admin">
    <node name="11" url="home.vspx"            id="11"  allowed="public guest reader author owner admin"/>
    <node name="12" url="search.vspx"          id="12"  allowed="public guest reader author owner admin"/>
    <node name="13" url="error.vspx"           id="13"  allowed="public guest reader author owner admin"/>
    <node name="14" url="settings.vspx"        id="14"  allowed="reader author owner admin"/>
  </node>
</menu_tree>';

  return S;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.xslt_root()
{
  declare sHost varchar;

  sHost := cast (registry_get('ab_path') as varchar);
  if (sHost = '0')
    return 'file://apps/AddressBook/xslt/';
  if (isnull (strstr(sHost, '/DAV/VAD')))
    return sprintf ('file://%sxslt/', sHost);
  return sprintf ('virt://WS.WS.SYS_DAV_RES.RES_FULL_PATH.RES_CONTENT:%sxslt/', sHost);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.xslt_full(
  in xslt_file varchar)
{
  return concat(AB.WA.xslt_root(), xslt_file);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.iri_fix (
  in S varchar)
{
  if (is_https_ctx ())
  {
    declare V any;

    V := rfc1808_parse_uri (S);
    V [0] := 'https';
    V [1] := http_request_header (http_request_header(), 'Host', null, registry_get ('URIQADefaultHost'));
    S := DB.DBA.vspx_uri_compose (V);
  }
  return S;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.url_fix (
  in S varchar,
  in sid varchar := null,
  in realm varchar := null)
{
  declare T varchar;

  T := '&';
  if (isnull (strchr (S, '?')))
  {
  T := '?';
  }
  if (not is_empty_or_null (sid))
  {
    S := S || T || 'sid=' || sid;
    T := '&';
  }
  if (not is_empty_or_null (realm))
  {
    S := S || T || 'realm=' || realm;
  }
  return S;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.exec (
  in S varchar,
  in P any := null)
{
  declare st, msg, meta, rows any;

  st := '00000';
  exec (S, st, msg, P, 0, meta, rows);
  if ('00000' = st)
    return rows;
  return vector ();
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_rss_sqlx_int (
  in domain_id integer,
  in account_id integer)
{
  declare retValue any;

  retValue := string_output ();

  http ('<?xml version ="1.0" encoding="UTF-8"?>\n', retValue);
  http ('<rss version="2.0">\n', retValue);
  http ('<channel>\n', retValue);

  http ('<sql:sqlx xmlns:sql="urn:schemas-openlink-com:xml-sql" sql:xsl=""><![CDATA[\n', retValue);
  http ('select \n', retValue);
  http ('  XMLELEMENT(\'title\', AB.WA.utf2wide(AB.WA.domain_name (<DOMAIN_ID>))), \n', retValue);
  http ('  XMLELEMENT(\'description\', AB.WA.utf2wide(AB.WA.domain_description (<DOMAIN_ID>))), \n', retValue);
  http ('  XMLELEMENT(\'managingEditor\', U_E_MAIL), \n', retValue);
  http ('  XMLELEMENT(\'pubDate\', AB.WA.dt_rfc1123(now())), \n', retValue);
  http ('  XMLELEMENT(\'generator\', \'Virtuoso Universal Server \' || sys_stat(\'st_dbms_ver\')), \n', retValue);
  http ('  XMLELEMENT(\'webMaster\', U_E_MAIL), \n', retValue);
  http ('  XMLELEMENT(\'link\', AB.WA.ab_url (<DOMAIN_ID>)) \n', retValue);
  http ('from DB.DBA.SYS_USERS where U_ID = <USER_ID> \n', retValue);
  http (']]></sql:sqlx>\n', retValue);

  http ('<sql:sqlx xmlns:sql=\'urn:schemas-openlink-com:xml-sql\'><![CDATA[\n', retValue);
  http ('select \n', retValue);
  http ('  XMLAGG(XMLELEMENT(\'item\', \n', retValue);
  http ('    XMLELEMENT(\'title\', AB.WA.utf2wide (P_NAME)), \n', retValue);
  http ('    XMLELEMENT(\'description\', AB.WA.utf2wide (P_FULL_NAME)), \n', retValue);
  http ('    XMLELEMENT(\'guid\', P_ID), \n', retValue);
  http ('    XMLELEMENT(\'link\', AB.WA.contact_url (<DOMAIN_ID>, P_ID)), \n', retValue);
  http ('    XMLELEMENT(\'pubDate\', AB.WA.dt_rfc1123 (P_UPDATED)), \n', retValue);
  http ('    (select XMLAGG (XMLELEMENT (\'category\', TV_TAG)) from AB..TAGS_VIEW where tags = P_TAGS), \n', retValue);
  http ('    XMLELEMENT(\'http://www.openlinksw.com/ods/:modified\', AB.WA.dt_iso8601 (P_UPDATED)))) \n', retValue);
  http ('from (select top 15  \n', retValue);
  http ('        P_NAME, \n', retValue);
  http ('        P_FULL_NAME, \n', retValue);
  http ('        P_UPDATED, \n', retValue);
  http ('        P_TAGS, \n', retValue);
  http ('        P_ID \n', retValue);
  http ('      from \n', retValue);
  http ('        AB.WA.PERSONS \n', retValue);
  http ('      where P_DOMAIN_ID = <DOMAIN_ID> \n', retValue);
  http ('      order by P_UPDATED desc) x \n', retValue);
  http (']]></sql:sqlx>\n', retValue);

  http ('</channel>\n', retValue);
  http ('</rss>\n', retValue);

  retValue := string_output_string (retValue);
  retValue := replace (retValue, '<USER_ID>', cast (account_id as varchar));
  retValue := replace (retValue, '<DOMAIN_ID>', cast (domain_id as varchar));
  return retValue;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_rss_sqlx (
  in domain_id integer,
  in account_id integer)
{
  declare retValue any;

  retValue := AB.WA.export_rss_sqlx_int (domain_id, account_id);
  return replace (retValue, 'sql:xsl=""', '');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_atom_sqlx(
  in domain_id integer,
  in account_id integer)
{
  declare retValue, xsltTemplate any;

  xsltTemplate := AB.WA.xslt_full ('rss2atom03.xsl');
  if (AB.WA.settings_atomVersion (AB.WA.settings (account_id)) = '1.0')
    xsltTemplate := AB.WA.xslt_full ('rss2atom.xsl');

  retValue := AB.WA.export_rss_sqlx_int (domain_id, account_id);
  return replace (retValue, 'sql:xsl=""', sprintf ('sql:xsl="%s"', xsltTemplate));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_rdf_sqlx (
  in domain_id integer,
  in account_id integer)
{
  declare retValue any;

  retValue := AB.WA.export_rss_sqlx_int (domain_id, account_id);
  return replace (retValue, 'sql:xsl=""', sprintf ('sql:xsl="%s"', AB.WA.xslt_full ('rss2rdf.xsl')));
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.export_comment_sqlx(
  in domain_id integer,
  in account_id integer)
{
  declare retValue any;

  retValue := string_output ();

  http ('<?xml version =\'1.0\' encoding=\'UTF-8\'?>\n', retValue);
  http ('<rss version="2.0" xmlns:sql=\'urn:schemas-openlink-com:xml-sql\'>\n', retValue);
  http ('<channel xmlns:wfw="http://wellformedweb.org/CommentAPI/" xmlns:dc="http://purl.org/dc/elements/1.1/">\n', retValue);

  http ('<sql:header><sql:param name=":id"> </sql:param></sql:header>\n', retValue);
  http ('<sql:sqlx><![CDATA[\n', retValue);
  http ('  select \n', retValue);
  http ('    XMLELEMENT (\'title\', AB.WA.utf2wide (P_NAME)), \n', retValue);
  http ('    XMLELEMENT (\'description\', AB.WA.utf2wide (AB.WA.xml2string(P_FULL_NAME))), \n', retValue);
  http ('    XMLELEMENT (\'link\', AB.WA.contact_url (<DOMAIN_ID>, P_ID)), \n', retValue);
  http ('    XMLELEMENT (\'pubDate\', AB.WA.dt_rfc1123 (P_CREATED)), \n', retValue);
  http ('    XMLELEMENT (\'generator\', \'Virtuoso Universal Server \' || sys_stat(\'st_dbms_ver\')), \n', retValue);
  http ('    XMLELEMENT (\'http://purl.org/dc/elements/1.1/:creator\', U_FULL_NAME) \n', retValue);
  http ('  from \n', retValue);
  http ('    AB.WA.PERSONS, DB.DBA.SYS_USERS, DB.DBA.WA_INSTANCE \n', retValue);
  http ('  where \n', retValue);
  http ('    P_ID = :id and U_ID = <USER_ID> and P_DOMAIN_ID = <DOMAIN_ID> and WAI_ID = P_DOMAIN_ID and WAI_IS_PUBLIC = 1\n', retValue);
  http (']]></sql:sqlx>\n', retValue);

  http ('<sql:sqlx><![CDATA[\n', retValue);
  http ('  select \n', retValue);
  http ('    XMLAGG (XMLELEMENT(\'item\',\n', retValue);
  http ('    XMLELEMENT (\'title\', AB.WA.utf2wide (PC_TITLE)),\n', retValue);
  http ('    XMLELEMENT (\'guid\', AB.WA.ab_url (<DOMAIN_ID>)||\'conversation.vspx\'||\'?id=\'||cast (PC_PERSON_ID as varchar)||\'#\'||cast (PC_ID as varchar)),\n', retValue);
  http ('    XMLELEMENT (\'link\', AB.WA.ab_url (<DOMAIN_ID>)||\'conversation.vspx\'||\'?id=\'||cast (PC_PERSON_ID as varchar)||\'#\'||cast (PC_ID as varchar)),\n', retValue);
  http ('    XMLELEMENT (\'http://purl.org/dc/elements/1.1/:creator\', PC_U_MAIL),\n', retValue);
  http ('    XMLELEMENT (\'pubDate\', DB.DBA.date_rfc1123 (PC_UPDATED)),\n', retValue);
  http ('    XMLELEMENT (\'description\', AB.WA.utf2wide (blob_to_string (PC_COMMENT))))) \n', retValue);
  http ('  from \n', retValue);
  http ('    (select TOP 15 \n', retValue);
  http ('       PC_ID, \n', retValue);
  http ('       PC_PERSON_ID, \n', retValue);
  http ('       PC_TITLE, \n', retValue);
  http ('       PC_COMMENT, \n', retValue);
  http ('       PC_U_MAIL, \n', retValue);
  http ('       PC_UPDATED \n', retValue);
  http ('     from \n', retValue);
  http ('       AB.WA.PERSON_COMMENTS, DB.DBA.WA_INSTANCE \n', retValue);
  http ('     where \n', retValue);
  http ('       PC_PERSON_ID = :id and WAI_ID = PC_DOMAIN_ID and WAI_IS_PUBLIC = 1\n', retValue);
  http ('     order by PC_UPDATED desc\n', retValue);
  http ('    ) sub \n', retValue);
  http (']]>\n', retValue);
  http ('</sql:sqlx>\n', retValue);

  http ('</channel>\n', retValue);
  http ('</rss>\n', retValue);

  retValue := string_output_string (retValue);
  retValue := replace (retValue, '<USER_ID>', cast(account_id as varchar));
  retValue := replace (retValue, '<DOMAIN_ID>', cast(domain_id as varchar));

  return retValue;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_gems_create (
  inout domain_id integer,
  inout account_id integer)
{
  declare read_perm, exec_perm, content, home, path varchar;

  home := AB.WA.dav_home (account_id);
  if (isnull (home))
    return;

  read_perm := '110100100N';
  exec_perm := '111101101N';
  home := home || 'Gems/';
  DB.DBA.DAV_MAKE_DIR (home, account_id, null, read_perm);

  home := home || AB.WA.domain_gems_name(domain_id) || '/';
  DB.DBA.DAV_MAKE_DIR (home, account_id, null, read_perm);

  -- RSS 2.0
  path := home || 'AddressBook.rss';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);

  content := AB.WA.export_rss_sqlx (domain_id, account_id);
  DB.DBA.DAV_RES_UPLOAD_STRSES_INT (path, content, 'text/xml', exec_perm, http_dav_uid (), http_dav_uid () + 1, null, null, 0);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-template', 'execute', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-encoding', 'utf-8', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-description', 'RSS based XML document generated by OpenLink AddressBook', 'dav', null, 0, 0, 1);

  -- ATOM
  path := home || 'AddressBook.atom';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);

  content := AB.WA.export_atom_sqlx (domain_id, account_id);
  DB.DBA.DAV_RES_UPLOAD_STRSES_INT (path, content, 'text/xml', exec_perm, http_dav_uid (), http_dav_uid () + 1, null, null, 0);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-template', 'execute', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-encoding', 'utf-8', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-description', 'ATOM based XML document generated by OpenLink AddressBook', 'dav', null, 0, 0, 1);

  -- RDF
  path := home || 'AddressBook.rdf';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);

  content := AB.WA.export_rdf_sqlx (domain_id, account_id);
  DB.DBA.DAV_RES_UPLOAD_STRSES_INT (path, content, 'text/xml', exec_perm, http_dav_uid (), http_dav_uid () + 1, null, null, 0);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-template', 'execute', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-encoding', 'utf-8', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-description', 'RDF based XML document generated by OpenLink AddressBook', 'dav', null, 0, 0, 1);

  -- COMMENT
  path := home || 'AddressBook.comment';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);

  content := AB.WA.export_comment_sqlx (domain_id, account_id);
  DB.DBA.DAV_RES_UPLOAD_STRSES_INT (path, content, 'text/xml', exec_perm, http_dav_uid (), http_dav_uid () + 1, null, null, 0);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-template', 'execute', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-encoding', 'utf-8', 'dav', null, 0, 0, 1);
  DB.DBA.DAV_PROP_SET_INT (path, 'xml-sql-description', 'RSS discussion based XML document generated by OpenLink AddressBook', 'dav', null, 0, 0, 1);

  return;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_gems_delete (
  in domain_id integer,
  in account_id integer := null,
  in appName varchar := 'AddressBook',
  in appGems varchar := null)
{
  declare tmp, home, appHome, path varchar;

  if (isnull (account_id))
    account_id := AB.WA.domain_owner_id (domain_id);

  home := AB.WA.dav_home (account_id);
  if (isnull (home))
    return;
  appHome := home || appName || '/';

  if (isnull (appGems))
    appGems := AB.WA.domain_gems_name (domain_id);
  home := appHome || appGems || '/';

  path := home || appName || '.rss';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);
  path := home || appName || '.rdf';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);
  path := home || appName || '.atom';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);
  path := home || appName || '.comment';
  DB.DBA.DAV_DELETE_INT (path, 1, null, null, 0);

  declare auth_uid, auth_pwd varchar;

  auth_uid := coalesce((SELECT U_NAME FROM WS.WS.SYS_DAV_USER WHERE U_ID = account_id), '');
  auth_pwd := coalesce((SELECT U_PWD FROM WS.WS.SYS_DAV_USER WHERE U_ID = account_id), '');
  if (auth_pwd[0] = 0)
    auth_pwd := pwd_magic_calc (auth_uid, auth_pwd, 1);

  tmp := DB.DBA.DAV_DIR_LIST (home, 0, auth_uid, auth_pwd);
  if (not isinteger(tmp) and not length(tmp))
    DB.DBA.DAV_DELETE_INT (home, 1, null, null, 0);

  tmp := DB.DBA.DAV_DIR_LIST (appHome, 0, auth_uid, auth_pwd);
  if (not isinteger(tmp) and not length(tmp))
    DB.DBA.DAV_DELETE_INT (appHome, 1, null, null, 0);

  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_update (
  inout domain_id integer,
  inout account_id integer)
{
  AB.WA.domain_gems_delete (domain_id, account_id, 'AddressBook', AB.WA.domain_gems_name (domain_id) || '_Gems');
  AB.WA.domain_gems_create (domain_id, account_id);

  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_owner_id (
  in domain_id integer)
{
  return (select A.WAM_USER from WA_MEMBER A, WA_INSTANCE B where A.WAM_MEMBER_TYPE = 1 and A.WAM_INST = B.WAI_NAME and B.WAI_ID = domain_id);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_owner_name (
  inout domain_id integer)
{
  return (select C.U_NAME from WA_MEMBER A, WA_INSTANCE B, SYS_USERS C where A.WAM_MEMBER_TYPE = 1 and A.WAM_INST = B.WAI_NAME and B.WAI_ID = domain_id and C.U_ID = A.WAM_USER);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_delete (
  in domain_id integer)
{
  delete from AB.WA.PERSONS where P_DOMAIN_ID = domain_id;
  delete from AB.WA.CATEGORIES where C_DOMAIN_ID = domain_id;
  delete from AB.WA.TAGS where T_DOMAIN_ID = domain_id;
  delete from AB.WA.EXCHANGE where EX_DOMAIN_ID = domain_id;
  delete from AB.WA.SETTINGS where S_DOMAIN_ID = domain_id;

  AB.WA.domain_gems_delete (domain_id);
  AB.WA.nntp_update (domain_id, null, null, 1, 0);

  VHOST_REMOVE(lpath => concat('/addressbook/', cast (domain_id as varchar)));

  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_id (
  in domain_name varchar)
{
  return (select WAI_ID from DB.DBA.WA_INSTANCE where WAI_NAME = domain_name);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_name (
  in domain_id integer)
{
  return coalesce((select WAI_NAME from DB.DBA.WA_INSTANCE where WAI_ID = domain_id), 'AddressBook Instance');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_nntp_name (
  in domain_id integer)
{
  return AB.WA.domain_nntp_name2 (AB.WA.domain_name (domain_id), AB.WA.domain_owner_name (domain_id));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_nntp_name2 (
  in domain_name varchar,
  in owner_name varchar)
{
  return sprintf ('ods.addressbook.%s.%U', owner_name, AB.WA.string2nntp (domain_name));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_gems_name (
  in domain_id integer)
{
  return concat(AB.WA.domain_name (domain_id), '');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_description (
  in domain_id integer)
{
  return coalesce((select coalesce(WAI_DESCRIPTION, WAI_NAME) from DB.DBA.WA_INSTANCE where WAI_ID = domain_id), 'AddressBook Instance');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_is_public (
  in domain_id integer)
{
  return coalesce((select WAI_IS_PUBLIC from DB.DBA.WA_INSTANCE where WAI_ID = domain_id), 0);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_ping (
  in domain_id integer)
{
  for (select WAI_NAME, WAI_DESCRIPTION from DB.DBA.WA_INSTANCE where WAI_ID = domain_id and WAI_IS_PUBLIC = 1) do
  {
    ODS..APP_PING (WAI_NAME, coalesce (WAI_DESCRIPTION, WAI_NAME), AB.WA.sioc_url (domain_id));
  }
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.forum_iri (
  in domain_id integer)
{
  return SIOC..addressbook_iri (AB.WA.domain_name (domain_id));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.domain_sioc_url (
  in domain_id integer,
  in sid varchar := null,
  in realm varchar := null)
{
  return AB.WA.url_fix (AB.WA.iri_fix (AB.WA.forum_iri (domain_id)), sid, realm);
}
;

-------------------------------------------------------------------------------
--
-- Account Functions
--
-------------------------------------------------------------------------------
create procedure AB.WA.account()
{
  declare vspx_user varchar;

  vspx_user := connection_get('owner_user');
  if (isnull (vspx_user))
    vspx_user := connection_get('vspx_user');
  return vspx_user;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_access (
  inout auth_uid varchar,
  inout auth_pwd varchar)
{
  if (isnull (auth_uid))
  auth_uid := AB.WA.account();
  auth_pwd := coalesce((SELECT U_PWD FROM WS.WS.SYS_DAV_USER WHERE U_NAME = auth_uid), '');
  if (auth_pwd[0] = 0)
    auth_pwd := pwd_magic_calc(auth_uid, auth_pwd, 1);
  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_delete(
  in domain_id integer,
  in account_id integer)
{
  declare iCount any;

  select count(WAM_USER) into iCount
    from WA_MEMBER,
         WA_INSTANCE
   where WAI_NAME = WAM_INST
     and WAI_TYPE_NAME = 'AddressBook'
     and WAM_USER = account_id;

  if (iCount = 0)
  {
    delete from AB.WA.GRANTS where G_GRANTER_ID = account_id or G_GRANTEE_ID = account_id;
  }

  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_id (
  in account_name varchar)
{
  return (select U_ID from DB.DBA.SYS_USERS where U_NAME = account_name);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_name (
  in account_id integer)
{
  return coalesce((select U_NAME from DB.DBA.SYS_USERS where U_ID = account_id), '');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_password (
  in account_id integer)
{
  return coalesce ((select pwd_magic_calc(U_NAME, U_PWD, 1) from WS.WS.SYS_DAV_USER where U_ID = account_id), '');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_fullName (
  in account_id integer)
{
  return coalesce ((select AB.WA.user_name (U_NAME, U_FULL_NAME) from DB.DBA.SYS_USERS where U_ID = account_id), '');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_mail(
  in account_id integer)
{
  return coalesce((select U_E_MAIL from DB.DBA.SYS_USERS where U_ID = account_id), '');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_sioc_url (
  in domain_id integer,
  in sid varchar := null,
  in realm varchar := null)
{
  declare S varchar;

  S := AB.WA.iri_fix (SIOC..person_iri (SIOC..user_iri (AB.WA.domain_owner_id (domain_id), null)));
  return AB.WA.url_fix (S, sid, realm);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.account_basicAuthorization (
  in account_id integer)
{
  declare account_name, account_password varchar;

  account_name := AB.WA.account_name (account_id);
  account_password := AB.WA.account_password (account_id);
  return sprintf ('Basic %s', encode_base64 (account_name || ':' || account_password));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.user_name(
  in u_name any,
  in u_full_name any) returns varchar
{
  if (not is_empty_or_null(trim(u_full_name)))
    return trim (u_full_name);
  return u_name;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.tag_prepare(
  inout tag varchar)
{
  if (not is_empty_or_null(tag))
  {
    tag := replace (trim (tag), '  ', ' ');
  }
  return tag;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.tag_delete(
  inout tags varchar,
  inout T integer)
{
  declare N integer;
  declare tags2 any;

  tags2 := AB.WA.tags2vector(tags);
  tags := '';
  for (N := 0; N < length(tags2); N := N + 1)
    if (N <> T)
      tags := concat(tags, ',', tags2[N]);
  return trim(tags, ',');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.tag_id (
  in tag varchar)
{
  tag := trim(tag);
  tag := replace (tag, ' ', '_');
  tag := replace (tag, '+', '_');
  return tag;
}
;

---------------------------------------------------------------------------------
--
create procedure AB.WA.tags_join(
  inout tags varchar,
  inout tags2 varchar)
{
  declare resultTags any;

  if (is_empty_or_null(tags))
    tags := '';
  if (is_empty_or_null(tags2))
    tags2 := '';

  resultTags := concat(tags, ',', tags2);
  resultTags := AB.WA.tags2vector(resultTags);
  resultTags := AB.WA.tags2unique(resultTags);
  resultTags := AB.WA.vector2tags(resultTags);
  return resultTags;
}
;

---------------------------------------------------------------------------------
--
create procedure AB.WA.tags2vector(
  inout tags varchar)
{
  return split_and_decode(trim(tags, ','), 0, '\0\0,');
}
;

---------------------------------------------------------------------------------
--
create procedure AB.WA.tags2search(
  in tags varchar)
{
  declare S varchar;
  declare V any;

  S := '';
  V := AB.WA.tags2vector(tags);
  foreach (any tag in V) do
    S := concat(S, ' ^T', replace (replace (trim(lcase(tag)), ' ', '_'), '+', '_'));
  return FTI_MAKE_SEARCH_STRING(trim(S, ','));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector2tags(
  inout aVector any)
{
  declare N integer;
  declare aResult any;

  aResult := '';
  for (N := 0; N < length(aVector); N := N + 1)
    if (N = 0) {
      aResult := trim(aVector[N]);
    } else {
      aResult := concat(aResult, ',', trim(aVector[N]));
    }
  return aResult;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.tags2unique(
  inout aVector any)
{
  declare aResult any;
  declare N, M integer;

  aResult := vector();
  for (N := 0; N < length(aVector); N := N + 1) {
    for (M := 0; M < length(aResult); M := M + 1)
      if (trim(lcase(aResult[M])) = trim(lcase(aVector[N])))
        goto _next;
    aResult := vector_concat(aResult, vector(trim(aVector[N])));
  _next:;
  }
  return aResult;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.tags_exchangeTest (
  inout tagsEvent any,
  inout tagsInclude any := null,
  inout tagsExclude any := null)
{
  declare N integer;
  declare tags, testTags any;

  if (is_empty_or_null (tagsEvent) and not is_empty_or_null (tagsInclude))
    goto _false;

  -- test exclude tags
  if (is_empty_or_null (tagsExclude))
    goto _include;
  if (is_empty_or_null (tagsEvent))
    goto _include;
  tags := AB.WA.tags2vector (tagsEvent);
  testTags := AB.WA.tags2vector (tagsExclude);
  for (N := 0; N < length (tags); N := N + 1)
  {
    if (AB.WA.vector_contains (testTags, tags [N]))
      goto _false;
  }

_include:;
  -- test include tags
  if (is_empty_or_null (tagsInclude))
    goto _true;
  tags := AB.WA.tags2vector (tagsEvent);
  testTags := AB.WA.tags2vector (tagsInclude);
  for (N := 0; N < length (tags); N := N + 1)
  {
    if (AB.WA.vector_contains (testTags, tags [N]))
      goto _true;
  }

_false:;
  return 0;

_true:;
  return 1;
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dav_home(
  inout account_id integer) returns varchar
{
  declare name, home any;
  declare cid integer;

  name := coalesce((select U_NAME from DB.DBA.SYS_USERS where U_ID = account_id), -1);
  if (isinteger(name))
    return null;
  home := AB.WA.dav_home_create(name);
  if (isinteger(home))
    return null;
  cid := DB.DBA.DAV_SEARCH_ID(home, 'C');
  if (isinteger(cid) and (cid > 0))
    return home;
  return null;
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dav_home_create(
  in user_name varchar) returns any
{
  declare user_id, cid integer;
  declare user_home varchar;

  whenever not found goto _error;

  if (is_empty_or_null(user_name))
    goto _error;
  user_home := DB.DBA.DAV_HOME_DIR(user_name);
  if (isstring(user_home))
    cid := DB.DBA.DAV_SEARCH_ID(user_home, 'C');
    if (isinteger(cid) and (cid > 0))
      return user_home;

  user_home := '/DAV/home/';
  DB.DBA.DAV_MAKE_DIR (user_home, http_dav_uid (), http_dav_uid () + 1, '110100100R');

  user_home := user_home || user_name || '/';
  user_id := (select U_ID from DB.DBA.SYS_USERS where U_NAME = user_name);
  DB.DBA.DAV_MAKE_DIR (user_home, user_id, null, '110100000R');
  USER_SET_OPTION(user_name, 'HOME', user_home);

  return user_home;

_error:
  return -18;
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dav_logical_home (
  inout account_id integer) returns varchar
{
  declare home any;

  home := AB.WA.dav_home (account_id);
  if (not isnull (home))
    home := replace (home, '/DAV', '');
  return home;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.host_protocol ()
{
  return case when is_https_ctx () then 'https://' else 'http://' end;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.host_url ()
{
  declare host varchar;

  declare exit handler for sqlstate '*' { goto _default; };

  if (is_http_ctx ())
  {
    host := http_request_header (http_request_header ( ) , 'Host' , null , sys_connected_server_address ());
    if (isstring (host) and strchr (host , ':') is null)
    {
      declare hp varchar;
      declare hpa any;

      hp := sys_connected_server_address ();
      hpa := split_and_decode ( hp , 0 , '\0\0:');
      host := host || ':' || hpa [1];
    }
    goto _exit;
  }

_default:;
  host := cfg_item_value (virtuoso_ini_path (), 'URIQA', 'DefaultHost');
  if (host is null)
  {
  host := sys_stat ('st_host_name');
  if (server_http_port () <> '80')
    host := host || ':' || server_http_port ();
  }

_exit:;
  if (host not like AB.WA.host_protocol () || '%')
    host := AB.WA.host_protocol () || host;

  return host;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_url (
  in domain_id integer)
{
  return concat(AB.WA.host_url(), '/addressbook/', cast (domain_id as varchar), '/');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.sioc_url (
  in domain_id integer)
{
  return sprintf ('http://%s/dataspace/%U/addressbook/%U/sioc.rdf', DB.DBA.wa_cname (), AB.WA.domain_owner_name (domain_id), replace (AB.WA.domain_name (domain_id), '+', '%2B'));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.gems_url (
  in domain_id integer)
{
  return sprintf ('http://%s/dataspace/%U/addressbook/%U/gems/', DB.DBA.wa_cname (), AB.WA.domain_owner_name (domain_id), replace (AB.WA.domain_name (domain_id), '+', '%2B'));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.foaf_url (
  in domain_id integer)
{
  return SIOC..person_iri (sprintf('http://%s%s/%s#this', SIOC..get_cname (), SIOC..get_base_path (), AB.WA.domain_owner_name (domain_id)), '/about.rdf');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_url (
  in domain_id integer,
  in person_id integer)
{
  return concat(AB.WA.ab_url (domain_id), 'home.vspx?id=', cast (person_id as varchar));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.dav_url (
  in domain_id integer)
{
  declare home varchar;

  home := AB.WA.dav_home (AB.WA.domain_owner_id (domain_id));
  if (isnull (home))
    return '';
  return concat('http://', DB.DBA.wa_cname (), home, 'AddressBook/', AB.WA.domain_gems_name (domain_id), '/');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.dav_url2 (
  in domain_id integer,
  in account_id integer)
{
  declare home varchar;

  home := AB.WA.dav_home(account_id);
  if (isnull (home))
    return '';
  return replace (concat(home, 'AddressBook/', AB.WA.domain_gems_name(domain_id), '/'), ' ', '%20');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.geo_url (
  in domain_id integer,
  in account_id integer)
{
  for (select WAUI_LAT, WAUI_LNG from WA_USER_INFO where WAUI_U_ID = account_id) do
    if ((not isnull (WAUI_LNG)) and (not isnull (WAUI_LAT)))
      return sprintf ('\n    <meta name="ICBM" content="%.2f, %.2f"><meta name="DC.title" content="%s">', WAUI_LNG, WAUI_LAT, AB.WA.domain_name (domain_id));
  return '';
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.banner_links (
  in domain_id integer,
  in sid varchar := null,
  in realm varchar := null)
{
  if (domain_id <= 0)
    return 'Public AddressBook';

  return sprintf ('<a href="%s" title="%s">%V</a> (<a href="%s" title="%s">%V</a>)',
                  AB.WA.domain_sioc_url (domain_id, sid, realm),
                  AB.WA.domain_name (domain_id),
                  AB.WA.domain_name (domain_id),
                  AB.WA.utf2wide (AB.WA.account_sioc_url (domain_id, sid, realm)),
                  AB.WA.account_fullName (AB.WA.domain_owner_id (domain_id)),
                  AB.WA.account_fullName (AB.WA.domain_owner_id (domain_id))
                 );
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.dav_content (
  in uri varchar,
  in auth_uid varchar := null,
  in auth_pwd varchar := null)
{
  declare cont varchar;
  declare hp any;

  declare exit handler for sqlstate '*'
  {
    --dbg_obj_print (__SQL_STATE, __SQL_MESSAGE);
    return null;
  };

  declare N integer;
  declare oldUri, newUri, reqHdr, resHdr varchar;

  newUri := replace (uri, ' ', '%20');
  reqHdr := null;
  if (isnull (auth_uid) or isnull (auth_pwd))
  AB.WA.account_access (auth_uid, auth_pwd);
  reqHdr := sprintf ('Authorization: Basic %s', encode_base64(auth_uid || ':' || auth_pwd));

_again:
  N := N + 1;
  oldUri := newUri;
  commit work;
  cont := http_get (newUri, resHdr, 'GET', reqHdr);
  if (resHdr[0] like 'HTTP/1._ 30_ %')
  {
    newUri := http_request_header (resHdr, 'Location');
    newUri := WS.WS.EXPAND_URL (oldUri, newUri);
    if (N > 15)
      return null;
    if (newUri <> oldUri)
      goto _again;
  }
  if (resHdr[0] like 'HTTP/1._ 4__ %' or resHdr[0] like 'HTTP/1._ 5__ %')
    return null;

  return (cont);
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.xml_set(
  in id varchar,
  inout pXml varchar,
  in value varchar)
{
  declare aEntity any;

  {
    declare exit handler for SQLSTATE '*'
    {
      pXml := xtree_doc('<?xml version="1.0" encoding="UTF-8"?><settings />');
      goto _skip;
    };
    if (not isentity(pXml))
      pXml := xtree_doc(pXml);
  }
_skip:
  aEntity := xpath_eval(sprintf ('/settings/entry[@ID = "%s"]', id), pXml);
  if (not isnull (aEntity))
    pXml := XMLUpdate(pXml, sprintf ('/settings/entry[@ID = "%s"]', id), null);

  if (not is_empty_or_null(value))
  {
    aEntity := xpath_eval('/settings', pXml);
    XMLAppendChildren(aEntity, xtree_doc(sprintf ('<entry ID="%s">%s</entry>', id, AB.WA.xml2string(AB.WA.utf2wide(value)))));
  }
  return pXml;
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.xml_get(
  in id varchar,
  inout pXml varchar,
  in defaultValue any := '')
{
  declare value any;

  declare exit handler for SQLSTATE '*' {return defaultValue;};

  if (not isentity(pXml))
    pXml := xtree_doc(pXml);
  value := xpath_eval (sprintf ('string(/settings/entry[@ID = "%s"]/.)', id), pXml);
  if (is_empty_or_null(value))
    return defaultValue;

  return AB.WA.wide2utf(value);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.string2xml (
  in content varchar,
  in mode integer := 0)
{
  if (mode = 0)
  {
    declare exit handler for sqlstate '*' { goto _html; };
    return xml_tree_doc (xml_tree (content, 0));
  }
_html:;
  return xml_tree_doc(xml_tree(content, 2, '', 'UTF-8'));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.xml2string(
  in pXml any)
{
  declare sStream any;

  sStream := string_output();
  http_value(pXml, null, sStream);
  return string_output_string(sStream);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.string2nntp (
  in S varchar)
{
  S := replace (S, '.', '_');
  S := replace (S, '@', '_');
  return sprintf ('%U', S);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.normalize_space(
  in S varchar)
{
  return xpath_eval ('normalize-space (string(/a))', XMLELEMENT('a', S), 1);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.utfClear(
  inout S varchar)
{
  declare N integer;
  declare retValue varchar;

  retValue := '';
  for (N := 0; N < length(S); N := N + 1)
  {
    if (S[N] <= 31)
    {
      retValue := concat(retValue, '?');
    } else {
      retValue := concat(retValue, chr(S[N]));
    }
  }
  return retValue;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.utf2wide (
  inout S any)
{
  if (isstring (S))
    return charset_recode (S, 'UTF-8', '_WIDE_');
  return S;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.wide2utf (
  inout S any)
{
  if (iswidestring (S))
    return charset_recode (S, '_WIDE_', 'UTF-8' );
  return charset_recode (S, null, 'UTF-8' );
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.stringCut (
  in S varchar,
  in L integer := 60)
{
  declare tmp any;

  if (not L)
    return S;
  tmp := AB.WA.utf2wide(S);
  if (not iswidestring(tmp))
    return S;
  if (length(tmp) > L)
    return AB.WA.wide2utf(concat(subseq(tmp, 0, L-3), '...'));
  return AB.WA.wide2utf(tmp);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_unique(
  inout aVector any,
  in minLength integer := 0)
{
  declare aResult any;
  declare N, M integer;

  aResult := vector();
  for (N := 0; N < length(aVector); N := N + 1)
  {
    if ((minLength = 0) or (length(aVector[N]) >= minLength))
    {
      for (M := 0; M < length(aResult); M := M + 1)
        if (trim(aResult[M]) = trim(aVector[N]))
          goto _next;
      aResult := vector_concat(aResult, vector(aVector[N]));
    }
  _next:;
  }
  return aResult;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_except(
  inout aVector any,
  inout aExcept any)
{
  declare aResult any;
  declare N, M integer;

  aResult := vector();
  for (N := 0; N < length(aVector); N := N + 1) {
    for (M := 0; M < length(aExcept); M := M + 1)
      if (aExcept[M] = aVector[N])
        goto _next;
    aResult := vector_concat(aResult, vector(trim(aVector[N])));
  _next:;
  }
  return aResult;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_contains(
  inout aVector any,
  in value varchar)
{
  declare N integer;

  for (N := 0; N < length(aVector); N := N + 1)
    if (value = aVector[N])
      return 1;
  return 0;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_index (
  inout aVector any,
  in value varchar)
{
  declare N integer;

  for (N := 0; N < length(aVector); N := N + 1)
    if (value = aVector[N])
      return N;
  return null;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_cut(
  inout aVector any,
  in value varchar)
{
  declare N integer;
  declare retValue any;

  retValue := vector();
  for (N := 0; N < length(aVector); N := N + 1)
    if (value <> aVector[N])
      retValue := vector_concat (retValue, vector(aVector[N]));
  return retValue;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_set (
  inout aVector any,
  in aIndex any,
  in aValue varchar)
{
  declare N integer;
  declare retValue any;

  retValue := vector();
  for (N := 0; N < length(aVector); N := N + 1)
    if (aIndex = N)
    {
      retValue := vector_concat (retValue, vector(aValue));
    } else {
      retValue := vector_concat (retValue, vector(aVector[N]));
    }
  return retValue;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_search(
  in aVector any,
  in value varchar,
  in condition varchar := 'AND')
{
  declare N integer;

  for (N := 0; N < length(aVector); N := N + 1)
    if (value like concat('%', aVector[N], '%')) {
      if (condition = 'OR')
        return 1;
    } else {
      if (condition = 'AND')
        return 0;
    }
  return 0;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector_reverse(
  in aVector any)
{
  declare aResult any;
  declare N integer;

  aResult := vector();
  for (N := length(aVector)-1; N >= 0; N := N - 1)
  {
    aResult := vector_concat (aResult, vector(aVector[N]));
  }
  return aResult;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector2str(
  inout aVector any,
  in delimiter varchar := ' ')
{
  declare tmp, aResult any;
  declare N integer;

  aResult := '';
  for (N := 0; N < length(aVector); N := N + 1)
  {
    tmp := trim(aVector[N]);
    if (strchr (tmp, ' ') is not null)
      tmp := concat('''', tmp, '''');
    if (N = 0) {
      aResult := tmp;
    } else {
      aResult := concat(aResult, delimiter, tmp);
    }
  }
  return aResult;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector2rs(
  inout aVector any)
{
  declare N integer;
  declare c0 varchar;

  result_names(c0);
  for (N := 0; N < length(aVector); N := N + 1)
    result(aVector[N]);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.pop (
  inout aVector any)
{
  declare N integer;
  declare retValue, V any;

  retValue := null;

  V := vector();
  for (N := 0; N < length(aVector); N := N + 1)
  {
    retValue := aVector[N];
    if (N <> length(aVector)-1)
      V := vector_concat (V, vector (retValue));
  }
  aVector := V;

  return retValue;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.push (
  inout aVector any,
  in aValue any)
{
  aVector := vector_concat (aVector, vector (aValue));
  return aVector;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.tagsDictionary2rs(
  inout aDictionary any)
{
  declare N integer;
  declare c0 varchar;
  declare c1 integer;
  declare V any;

  V := dict_to_vector(aDictionary, 1);
  result_names(c0, c1);
  for (N := 1; N < length(V); N := N + 2)
    result(V[N][0], V[N][1]);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.dictionary2rs(
  inout aDictionary any)
{
  declare N integer;
  declare tmp, c0, c1 varchar;
  declare V any;

  V := dict_to_vector(aDictionary, 1);
  result_names(c0, c1);
  for (N := 0; N < length (V); N := N + 2)
  {
    tmp := V[N+1];
    if (__tag (tmp) = 193)
      tmp := serialize (tmp);
    tmp := cast (tmp as varchar);
    result(V[N], tmp);
  }
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.vector2src(
  inout aVector any)
{
  declare N integer;
  declare aResult any;

  aResult := 'vector(';
  for (N := 0; N < length(aVector); N := N + 1)
  {
    if (N = 0)
      aResult := concat(aResult, '''', trim(aVector[N]), '''');
    if (N <> 0)
      aResult := concat(aResult, ', ''', trim(aVector[N]), '''');
  }
  return concat(aResult, ')');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ft2vector(
  in S any)
{
  declare w varchar;
  declare aResult any;

  aResult := vector();
  w := regexp_match ('["][^"]+["]|[''][^'']+['']|[^"'' ]+', S, 1);
  while (w is not null) {
    w := trim (w, '"'' ');
    if (upper(w) not in ('AND', 'NOT', 'NEAR', 'OR') and length (w) > 1 and not vt_is_noise (AB.WA.wide2utf(w), 'utf-8', 'x-ViDoc'))
      aResult := vector_concat(aResult, vector(w));
    w := regexp_match ('["][^"]+["]|[''][^'']+['']|[^"'' ]+', S, 1);
  }
  return aResult;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.set_keyword (
  in    name   varchar,
  inout params any,
  in    value  any)
{
  declare N integer;

  for (N := 0; N < length(params); N := N + 2)
  {
    if (params[N] = name)
    {
      params[N+1] := value;
      goto _end;
    }
  }
  params := vector_concat(params, vector(name, value));
_end:
  return params;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_tree2(
  in domain_id integer,
  in user_id integer,
  in node varchar,
  in path varchar)
{
  declare node_type, node_id any;

  node_id := AB.WA.node_id(node);
  node_type := AB.WA.node_type(node);
  if (node_type = 'r')
  {
    if (node_id = 2)
      return vector('Shared Contacts By', AB.WA.make_node ('u', -1), AB.WA.make_path(path, 'u', -1));
  }

  declare retValue any;
  retValue := vector ();

  if ((node_type = 'u') and (node_id = -1))
    for (select distinct U_ID, U_NAME from AB.WA.GRANTS, DB.DBA.SYS_USERS where G_GRANTEE_ID = user_id and G_GRANTER_ID = U_ID order by 2) do
      retValue := vector_concat(retValue, vector(U_NAME, AB.WA.make_node ('u', U_ID), AB.WA.make_path(path, 'u', U_ID)));

  return retValue;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_node_has_childs (
  in domain_id integer,
  in user_id integer,
  in node varchar,
  in path varchar)
{
  declare node_type, node_id any;

  node_id := AB.WA.node_id(node);
  node_type := AB.WA.node_type(node);

  if ((node_type = 'u') and (node_id = -1))
    if (exists (select 1 from AB.WA.GRANTS, DB.DBA.SYS_USERS where G_GRANTEE_ID = user_id and G_GRANTER_ID = U_ID))
      return 1;

  return 0;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_path2_int(
  in node varchar,
  inout path varchar)
{
  declare node_type, node_id any;

  node_id := AB.WA.node_id(node);
  node_type := AB.WA.node_type(node);

  if ((node_type = 'u') and (node_id >= 0))
    path := sprintf('%s/%s', AB.WA.make_node (node_type, -1), path);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_path2 (
  in node varchar,
  in grant_id integer)
{
  declare user_id, root_id any;
  declare path any;

  path := node;
  user_id := (select G_GRANTER_ID from AB.WA.GRANTS where G_ID = grant_id);
  AB.WA.ab_path2_int (node, root_id, path);
  return '/' || path;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.make_node (
  in node_type varchar,
  in node_id any)
{
  return node_type || '#' || cast(node_id as varchar);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.make_path (
  in path varchar,
  in node_type varchar,
  in node_id any)
{
  return path || '/' || AB.WA.make_node (node_type, node_id);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.node_type(
  in code varchar)
{
  if ((length(code) > 1) and (substring(code,2,1) = '#'))
    return left(code, 1);
  return '';
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.node_id (
  in node varchar)
{
  declare exit handler for sqlstate '*' { return -1; };

  if ((length(node) > 2) and (substring(node,2,1) = '#'))
    return cast(subseq(node, 2) as integer);
  return 0;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.node_suffix (
  in node varchar)
{
  if ((length(node) > 2) and (substring(node,2,1) = '#'))
    return subseq(node, 2);
  return '';
}
;

-------------------------------------------------------------------------------
--
-- Show functions
--
-------------------------------------------------------------------------------
--
create procedure AB.WA.show_text(
  in S any,
  in S2 any)
{
  if (isstring(S))
    S := trim(S);
  if (is_empty_or_null(S))
    return sprintf ('~ no %s ~', S2);
  return S;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.show_title(
  in S any)
{
  return AB.WA.show_text(S, 'title');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.show_author(
  in S any)
{
  return AB.WA.show_text(S, 'author');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.show_description(
  in S any)
{
  return AB.WA.show_text(S, 'description');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.show_excerpt(
  in S varchar,
  in words varchar)
{
  return coalesce(search_excerpt (words, cast (S as varchar)), '');
}
;

-------------------------------------------------------------------------------
--
-- Date / Time functions
--
-------------------------------------------------------------------------------
-- returns system time in GMT
--
create procedure AB.WA.dt_current_time()
{
  return dateadd('minute', - timezone(now()), now());
}
;

-------------------------------------------------------------------------------
--
-- convert from GMT date to user timezone;
--
create procedure AB.WA.dt_gmt2user(
  in pDate datetime,
  in pUser varchar := null)
{
  declare tz integer;

  if (isnull (pDate))
    return null;
  if (isnull (pUser))
    pUser := connection_get('owner_user');
  if (isnull (pUser))
    pUser := connection_get('vspx_user');
  if (is_empty_or_null (pUser))
    return pDate;
  tz := cast (coalesce(USER_GET_OPTION(pUser, 'TIMEZONE'), timezone(now())/60) as integer) * 60;
  return dateadd('minute', tz, pDate);
}
;

-------------------------------------------------------------------------------
--
-- convert from the user timezone to GMT date
--
create procedure AB.WA.dt_user2gmt(
  in pDate datetime,
  in pUser varchar := null)
{
  declare tz integer;

  if (isnull (pDate))
    return null;
  if (isnull (pUser))
    pUser := connection_get('owner_user');
  if (isnull (pUser))
    pUser := connection_get('vspx_user');
  if (isnull (pUser))
    return pDate;
  tz := cast (coalesce(USER_GET_OPTION(pUser, 'TIMEZONE'), 0) as integer) * 60;
  return dateadd('minute', -tz, pDate);
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dt_value (
  in pDate datetime,
  in pUser datetime := null)
{
  if (isnull (pDate))
    return pDate;
  pDate := AB.WA.dt_gmt2user(pDate, pUser);
  if (AB.WA.dt_format(pDate, 'D.M.Y') = AB.WA.dt_format(now(), 'D.M.Y'))
    return concat('today ', AB.WA.dt_format(pDate, 'H:N'));
  return AB.WA.dt_format(pDate, 'D.M.Y H:N');
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dt_date (
  in pDate datetime,
  in pUser datetime := null)
{
  if (isnull (pDate))
    return pDate;
  pDate := AB.WA.dt_gmt2user(pDate, pUser);
  return AB.WA.dt_format(pDate, 'D.M.Y');
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dt_format(
  in pDate datetime,
  in pFormat varchar := 'd.m.Y')
{
  declare N integer;
  declare ch, S varchar;

  declare exit handler for sqlstate '*' {
    return '';
  };

  S := '';
  N := 1;
  while (N <= length(pFormat))
  {
    ch := substring(pFormat, N, 1);
    if (ch = 'M')
    {
      S := concat(S, xslt_format_number(month(pDate), '00'));
    } else {
      if (ch = 'm')
      {
        S := concat(S, xslt_format_number(month(pDate), '##'));
      } else
      {
        if (ch = 'Y')
        {
          S := concat(S, xslt_format_number(year(pDate), '0000'));
        } else
        {
          if (ch = 'y')
          {
            S := concat(S, substring(xslt_format_number(year(pDate), '0000'),3,2));
          } else {
            if (ch = 'd')
            {
              S := concat(S, xslt_format_number(dayofmonth(pDate), '##'));
            } else
            {
              if (ch = 'D')
              {
                S := concat(S, xslt_format_number(dayofmonth(pDate), '00'));
              } else
              {
                if (ch = 'H')
                {
                  S := concat(S, xslt_format_number(hour(pDate), '00'));
                } else
                {
                  if (ch = 'h')
                  {
                    S := concat(S, xslt_format_number(hour(pDate), '##'));
                  } else
                  {
                    if (ch = 'N')
                    {
                      S := concat(S, xslt_format_number(minute(pDate), '00'));
                    } else
                    {
                      if (ch = 'n')
                      {
                        S := concat(S, xslt_format_number(minute(pDate), '##'));
                      } else
                      {
                        if (ch = 'S')
                        {
                          S := concat(S, xslt_format_number(second(pDate), '00'));
                        } else
                        {
                          if (ch = 's')
                          {
                            S := concat(S, xslt_format_number(second(pDate), '##'));
                          } else
                          {
                            S := concat(S, ch);
                          };
                        };
                      };
                    };
                  };
                };
              };
            };
          };
        };
      };
    };
    N := N + 1;
  };
  return S;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.dt_deformat(
  in pString varchar,
  in pFormat varchar := 'd.m.Y')
{
  declare y, m, d integer;
  declare N, I integer;
  declare ch varchar;

  N := 1;
  I := 0;
  d := 0;
  m := 0;
  y := 0;
  while (N <= length(pFormat)) {
    ch := upper(substring(pFormat, N, 1));
    if (ch = 'M')
      m := AB.WA.dt_deformat_tmp(pString, I);
    if (ch = 'D')
      d := AB.WA.dt_deformat_tmp(pString, I);
    if (ch = 'Y') {
      y := AB.WA.dt_deformat_tmp(pString, I);
      if (y < 50)
        y := 2000 + y;
      if (y < 100)
        y := 1900 + y;
    };
    N := N + 1;
  };
  return stringdate(concat(cast (m as varchar), '.', cast (d as varchar), '.', cast (y as varchar)));
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dt_deformat_tmp(
  in S varchar,
  inout N varchar)
{
  declare
    V any;

  V := regexp_parse('[0-9]+', S, N);
  if (length(V) > 1) {
    N := aref(V,1);
    return atoi(subseq(S, aref(V, 0), aref(V,1)));
  };
  N := N + 1;
  return 0;
}
;

-----------------------------------------------------------------------------
--
create procedure AB.WA.dt_reformat(
  in pString varchar,
  in pInFormat varchar := 'd.m.Y',
  in pOutFormat varchar := 'm.d.Y')
{
  return AB.WA.dt_format(AB.WA.dt_deformat(pString, pInFormat), pOutFormat);
};

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.dt_convert(
  in pString varchar,
  in pDefault any := null)
{
  declare exit handler for sqlstate '*' { goto _next; };
  return stringdate(pString);
_next:
  declare exit handler for sqlstate '*' { goto _end; };
  return http_string_date(pString);

_end:
  return pDefault;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.dt_rfc1123 (
  in dt datetime)
{
  return soap_print_box (dt, '', 1);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.dt_iso8601 (
  in dt datetime)
{
  return soap_print_box (dt, '', 0);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.test_clear (
  in S any)
{
  declare N integer;

  return substring(S, 1, coalesce(strstr(S, '<>'), length(S)));
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.test (
  in value any,
  in params any := null)
{
  declare valueType, valueClass, valueName, valueMessage, tmp any;

  declare exit handler for SQLSTATE '*' {
    if (not is_empty_or_null(valueMessage))
      signal ('TEST', valueMessage);
    if (__SQL_STATE = 'EMPTY')
      signal ('TEST', sprintf ('Field ''%s'' cannot be empty!<>', valueName));
    if (__SQL_STATE = 'CLASS') {
      if (valueType in ('free-text', 'tags')) {
        signal ('TEST', sprintf ('Field ''%s'' contains invalid characters or noise words!<>', valueName));
      } else {
        signal ('TEST', sprintf ('Field ''%s'' contains invalid characters!<>', valueName));
      }
    }
    if (__SQL_STATE = 'TYPE')
      signal ('TEST', sprintf ('Field ''%s'' contains invalid characters for \'%s\'!<>', valueName, valueType));
    if (__SQL_STATE = 'MIN')
      signal ('TEST', sprintf ('''%s'' value should be greater than %s!<>', valueName, cast (tmp as varchar)));
    if (__SQL_STATE = 'MAX')
      signal ('TEST', sprintf ('''%s'' value should be less than %s!<>', valueName, cast (tmp as varchar)));
    if (__SQL_STATE = 'MINLENGTH')
      signal ('TEST', sprintf ('The length of field ''%s'' should be greater than %s characters!<>', valueName, cast (tmp as varchar)));
    if (__SQL_STATE = 'MAXLENGTH')
      signal ('TEST', sprintf ('The length of field ''%s'' should be less than %s characters!<>', valueName, cast (tmp as varchar)));
    if (__SQL_STATE = 'SPECIAL')
      signal ('TEST', __SQL_MESSAGE || '<>');
    signal ('TEST', 'Unknown validation error!<>');
    --resignal;
  };

  if (isstring (value))
  value := trim(value);
  if (is_empty_or_null(params))
    return value;

  valueClass := coalesce(get_keyword('class', params), get_keyword('type', params));
  valueType := coalesce(get_keyword('type', params), get_keyword('class', params));
  valueName := get_keyword('name', params, 'Field');
  valueMessage := get_keyword('message', params, '');
  tmp := get_keyword('canEmpty', params);
  if (isnull (tmp))
  {
    if (not isnull (get_keyword ('minValue', params)))
    {
      tmp := 0;
    } else if (get_keyword('minLength', params, 0) <> 0) {
      tmp := 0;
    }
  }
  if (not isnull (tmp) and (tmp = 0) and is_empty_or_null (value))
  {
    signal('EMPTY', '');
  } else if (is_empty_or_null(value)) {
    return value;
  }
  value := AB.WA.validate2 (valueClass, cast (value as varchar));
  if (valueType = 'integer') {
    tmp := get_keyword('minValue', params);
    if ((not isnull (tmp)) and (value < tmp))
      signal('MIN', cast (tmp as varchar));

    tmp := get_keyword('maxValue', params);
    if (not isnull (tmp) and (value > tmp))
      signal('MAX', cast (tmp as varchar));

  } else if (valueType = 'float') {
    tmp := get_keyword('minValue', params);
    if (not isnull (tmp) and (value < tmp))
      signal('MIN', cast (tmp as varchar));

    tmp := get_keyword('maxValue', params);
    if (not isnull (tmp) and (value > tmp))
      signal('MAX', cast (tmp as varchar));

  } else if (valueType = 'varchar') {
    tmp := get_keyword('minLength', params);
    if (not isnull (tmp) and (length(AB.WA.utf2wide(value)) < tmp))
      signal('MINLENGTH', cast (tmp as varchar));

    tmp := get_keyword('maxLength', params);
    if (not isnull (tmp) and (length(AB.WA.utf2wide(value)) > tmp))
      signal('MAXLENGTH', cast (tmp as varchar));
  }
  return value;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.validate2 (
  in propertyType varchar,
  in propertyValue varchar)
{
  declare exit handler for SQLSTATE '*' {
    if (__SQL_STATE = 'CLASS')
      resignal;
    if (__SQL_STATE = 'SPECIAL')
      resignal;
    signal('TYPE', propertyType);
    return;
  };

  if (propertyType = 'boolean') {
    if (propertyValue not in ('Yes', 'No'))
      goto _error;
  } else if (propertyType = 'integer') {
    if (isnull (regexp_match('^[0-9]+\$', propertyValue)))
      goto _error;
    return cast (propertyValue as integer);
  } else if (propertyType = 'float') {
    if (isnull (regexp_match('^[-+]?([0-9]*\.)?[0-9]+([eE][-+]?[0-9]+)?\$', propertyValue)))
      goto _error;
    return cast (propertyValue as float);
  } else if (propertyType = 'dateTime') {
    if (isnull (regexp_match('^((?:19|20)[0-9][0-9])[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]) ([01]?[0-9]|[2][0-3])(:[0-5][0-9])?\$', propertyValue)))
      goto _error;
    return cast (propertyValue as datetime);
  } else if (propertyType = 'dateTime2') {
    if (isnull (regexp_match('^((?:19|20)[0-9][0-9])[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]) ([01]?[0-9]|[2][0-3])(:[0-5][0-9])?\$', propertyValue)))
      goto _error;
    return cast (propertyValue as datetime);
  } else if (propertyType = 'date') {
    if (isnull (regexp_match('^((?:19|20)[0-9][0-9])[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])\$', propertyValue)))
      goto _error;
    return cast (propertyValue as datetime);
  } else if (propertyType = 'date2') {
    if (isnull (regexp_match('^(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|1[012])[- /.]((?:19|20)[0-9][0-9])\$', propertyValue)))
      goto _error;
    return stringdate(AB.WA.dt_reformat(propertyValue, 'd.m.Y', 'Y-M-D'));
  } else if (propertyType = 'time') {
    if (isnull (regexp_match('^([01]?[0-9]|[2][0-3])(:[0-5][0-9])?\$', propertyValue)))
      goto _error;
    return cast (propertyValue as time);
  } else if (propertyType = 'folder') {
    if (isnull (regexp_match('^[^\\\/\?\*\"\'\>\<\:\|]*\$', propertyValue)))
      goto _error;
  } else if ((propertyType = 'uri') or (propertyType = 'anyuri')) {
    if (isnull (regexp_match('^(ht|f)tp(s?)\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(:(0-9)*)*(\/?)([a-zA-Z0-9\-\.\?\,\'\/\\\+&amp;%\$#_=:]*)?\$', propertyValue)))
      goto _error;
  } else if (propertyType = 'email') {
    if (isnull (regexp_match('^([a-zA-Z0-9_\-])+(\.([a-zA-Z0-9_\-])+)*@((\[(((([0-1])?([0-9])?[0-9])|(2[0-4][0-9])|(2[0-5][0-5])))\.(((([0-1])?([0-9])?[0-9])|(2[0-4][0-9])|(2[0-5][0-5])))\.(((([0-1])?([0-9])?[0-9])|(2[0-4][0-9])|(2[0-5][0-5])))\.(((([0-1])?([0-9])?[0-9])|(2[0-4][0-9])|(2[0-5][0-5]))\]))|((([a-zA-Z0-9])+(([\-])+([a-zA-Z0-9])+)*\.)+([a-zA-Z])+(([\-])+([a-zA-Z0-9])+)*))\$', propertyValue)))
      goto _error;
  } else if (propertyType = 'free-text') {
    if (length(propertyValue))
      if (not AB.WA.validate_freeTexts(propertyValue))
        goto _error;
  } else if (propertyType = 'free-text-expression') {
    if (length(propertyValue))
      if (not AB.WA.validate_freeText(propertyValue))
        goto _error;
  } else if (propertyType = 'tags') {
    if (not AB.WA.validate_tags(propertyValue))
      goto _error;
  }
  return propertyValue;

_error:
  signal('CLASS', propertyType);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.validate (
  in propertyType varchar,
  in propertyValue varchar,
  in propertyEmpty integer := 1)
{
  if (is_empty_or_null(propertyValue))
    return propertyEmpty;

  declare tmp any;
  declare exit handler for SQLSTATE '*' {return 0;};

  if (propertyType = 'boolean') {
    if (propertyValue not in ('Yes', 'No'))
      return 0;
  } else if (propertyType = 'integer') {
    if (isnull (regexp_match('^[0-9]+\$', propertyValue)))
      return 0;
    tmp := cast (propertyValue as integer);
  } else if (propertyType = 'float') {
    if (isnull (regexp_match('^[-+]?([0-9]*\.)?[0-9]+([eE][-+]?[0-9]+)?\$', propertyValue)))
      return 0;
    tmp := cast (propertyValue as float);
  } else if (propertyType = 'dateTime') {
    if (isnull (regexp_match('^((?:19|20)[0-9][0-9])[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]) ([01]?[0-9]|[2][0-3])(:[0-5][0-9])?\$', propertyValue)))
      return 0;
  } else if (propertyType = 'dateTime2') {
    if (isnull (regexp_match('^((?:19|20)[0-9][0-9])[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01]) ([01]?[0-9]|[2][0-3])(:[0-5][0-9])?\$', propertyValue)))
      return 0;
  } else if (propertyType = 'date') {
    if (isnull (regexp_match('^((?:19|20)[0-9][0-9])[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])\$', propertyValue)))
      return 0;
  } else if (propertyType = 'date2') {
    if (isnull (regexp_match('^(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|1[012])[- /.]((?:19|20)[0-9][0-9])\$', propertyValue)))
      return 0;
  } else if (propertyType = 'time') {
    if (isnull (regexp_match('^([01]?[0-9]|[2][0-3])(:[0-5][0-9])?\$', propertyValue)))
      return 0;
  } else if (propertyType = 'folder') {
    if (isnull (regexp_match('^[^\\\/\?\*\"\'\>\<\:\|]*\$', propertyValue)))
      return 0;
  } else if (propertyType = 'uri') {
    if (isnull (regexp_match('^(ht|f)tp(s?)\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(:(0-9)*)*(\/?)([a-zA-Z0-9\-\.\?\,\'\/\\\+&amp;%\$#_]*)?\$', propertyValue)))
      return 0;
  }
  return 1;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.validate_freeText (
  in S varchar)
{
  declare st, msg varchar;

  if (upper(S) in ('AND', 'NOT', 'NEAR', 'OR'))
    return 0;
  if (length (S) < 2)
    return 0;
  if (vt_is_noise (AB.WA.wide2utf(S), 'utf-8', 'x-ViDoc'))
    return 0;
  st := '00000';
  exec (sprintf ('vt_parse (\'[__lang "x-ViDoc" __enc "utf-8"] %s\')', S), st, msg, vector ());
  if (st <> '00000')
    return 0;
  return 1;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.validate_freeTexts (
  in S any)
{
  declare w varchar;

  w := regexp_match ('["][^"]+["]|[''][^'']+['']|[^"'' ]+', S, 1);
  while (w is not null) {
    w := trim (w, '"'' ');
    if (not AB.WA.validate_freeText(w))
      return 0;
    w := regexp_match ('["][^"]+["]|[''][^'']+['']|[^"'' ]+', S, 1);
  }
  return 1;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.validate_tag (
  in T varchar)
{
  declare S any;
  
  S := T;
  S := replace (trim(S), '+', '_');
  S := replace (trim(S), ' ', '_');
  if (not AB.WA.validate_freeText(S))
    return 0;
  if (not isnull (strstr(S, '"')))
    return 0;
  if (not isnull (strstr(S, '''')))
    return 0;
  if (length(S) < 2)
    return 0;
  if (length(S) > 50)
    return 0;
  return 1;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.validate_tags (
  in S varchar)
{
  declare N integer;
  declare V any;

  V := AB.WA.tags2vector(S);
  if (is_empty_or_null(V))
    return 0;
  if (length(V) <> length(AB.WA.tags2unique(V)))
    return 0;
  for (N := 0; N < length(V); N := N + 1)
    if (not AB.WA.validate_tag(V[N]))
      return 0;
  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_sparql (
  in S varchar,
  in debug any := null)
{
  declare st, msg, meta, rows any;

  st := '00000';
  exec (S, st, msg, vector (), 0, meta, rows);
  if (not isnull (debug) and ('00000' <> st))
    dbg_obj_print ('', S, st, msg);
  if ('00000' = st)
    return rows;
  return vector ();
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_graph_delete (
  in graph varchar)
{
  AB.WA.ab_sparql (sprintf ('SPARQL clear graph <%s>', graph));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.ab_graph_create ()
{
  return 'http://local.virt/addressbook/' || cast (rnd (1000) as varchar);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.dashboard_get(
  in domain_id integer,
  in user_id integer)
{
  declare ses any;

  ses := string_output ();
  http ('<ab-db>', ses);
  for select top 10 *
        from (select a.P_NAME,
                     SIOC..addressbook_contact_iri (domain_id, P_ID) P_URI,
                     coalesce (a.P_UPDATED, now ()) P_UPDATED
                from AB.WA.PERSONS a,
                     DB.DBA.WA_INSTANCE b,
                     DB.DBA.WA_MEMBER c
                where a.P_DOMAIN_ID = domain_id
                  and b.WAI_ID = a.P_DOMAIN_ID
                  and c.WAM_INST = b.WAI_NAME
                  and c.WAM_USER = user_id
                order by a.P_UPDATED desc
             ) x do
  {
    declare uname, full_name varchar;

    uname := (select coalesce (U_NAME, '') from DB.DBA.SYS_USERS where U_ID = user_id);
    full_name := (select coalesce (coalesce (U_FULL_NAME, U_NAME), '') from DB.DBA.SYS_USERS where U_ID = user_id);

    http ('<ab>', ses);
    http (sprintf ('<dt>%s</dt>', date_iso8601 (P_UPDATED)), ses);
    http (sprintf ('<title><![CDATA[%s]]></title>', P_NAME), ses);
    http (sprintf ('<link><![CDATA[%s]]></link>', P_URI), ses);
    http (sprintf ('<from><![CDATA[%s]]></from>', full_name), ses);
    http (sprintf ('<uid>%s</uid>', uname), ses);
    http ('</ab>', ses);
  }
  http ('</ab-db>', ses);
  return string_output_string (ses);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.settings (
  inout domain_id integer)
{
  return coalesce ((select deserialize (blob_to_string (S_DATA)) from AB.WA.SETTINGS where S_DOMAIN_ID = domain_id), vector());
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.settings_init (
  inout settings any)
{
  AB.WA.set_keyword ('chars', settings, cast (get_keyword ('chars', settings, '60') as integer));
  AB.WA.set_keyword ('rows', settings, cast (get_keyword ('rows', settings, '10') as integer));
  AB.WA.set_keyword ('tbLabels', settings, cast (get_keyword ('tbLabels', settings, '1') as integer));
  AB.WA.set_keyword ('atomVersion', settings, get_keyword ('atomVersion', settings, '1.0'));
  AB.WA.set_keyword ('conv', settings, cast (get_keyword ('conv', settings, '0') as integer));
  AB.WA.set_keyword ('conv_init', settings, cast (get_keyword ('conv_init', settings, '0') as integer));
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.settings_rows (
  in settings any)
{
  return cast (get_keyword('rows', settings, '10') as integer);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.settings_atomVersion (
  in settings any)
{
  return get_keyword('atomVersion', settings, '1.0');
}
;

-----------------------------------------------------------------------------------------
--
-- Contacts
--
-----------------------------------------------------------------------------------------
create procedure AB.WA.contact_update (
  in id integer,
  in domain_id integer,
  in category_id integer,
  in kind integer,
  in name varchar,
  in title varchar,
  in fName varchar,
  in mName varchar,
  in lName varchar,
  in fullName varchar,
  in gender varchar,
  in birthday datetime,
  in iri varchar,
  in foaf varchar,
  in photo varchar,
  in interests varchar,
  in relationships varchar,
  in mail varchar,
  in web varchar,
  in icq varchar,
  in skype varchar,
  in aim varchar,
  in yahoo varchar,
  in msn varchar,
  in hCountry varchar,
  in hState varchar,
  in hCity varchar,
  in hCode varchar,
  in hAddress1 varchar,
  in hAddress2 varchar,
  in hTzone varchar,
  in hLat real,
  in hLng real,
  in hPhone varchar,
  in hMobile varchar,
  in hFax varchar,
  in hMail varchar,
  in hWeb varchar,
  in bCountry varchar,
  in bState varchar,
  in bCity varchar,
  in bCode varchar,
  in bAddress1 varchar,
  in bAddress2 varchar,
  in bTzone varchar,
  in bLat real,
  in bLng real,
  in bPhone varchar,
  in bMobile varchar,
  in bFax varchar,
  in bIndustry varchar,
  in bOrganization varchar,
  in bDepartment varchar,
  in bJob varchar,
  in bMail varchar,
  in bWeb varchar,
  in tags varchar)
{
  if (id = -1)
  {
    id := sequence_next ('AB.WA.contact_id');
    insert into AB.WA.PERSONS
      (
        P_ID,
        P_DOMAIN_ID,
        P_CATEGORY_ID,
        P_KIND,
        P_NAME,
        P_TITLE,
        P_FIRST_NAME,
        P_MIDDLE_NAME,
        P_LAST_NAME,
        P_FULL_NAME,
        P_GENDER,
        P_BIRTHDAY,
        P_IRI,
        P_FOAF,
        P_PHOTO,
        P_INTERESTS,
        P_MAIL,
        P_WEB,
        P_ICQ,
        P_SKYPE,
        P_AIM,
        P_YAHOO,
        P_MSN,
        P_H_COUNTRY,
        P_H_STATE,
        P_H_CITY,
        P_H_CODE,
        P_H_ADDRESS1,
        P_H_ADDRESS2,
        P_H_TZONE,
        P_H_LAT,
        P_H_LNG,
        P_H_PHONE,
        P_H_MOBILE,
        P_H_FAX,
        P_H_MAIL,
        P_H_WEB,
        P_B_COUNTRY,
        P_B_STATE,
        P_B_CITY,
        P_B_CODE,
        P_B_ADDRESS1,
        P_B_ADDRESS2,
        P_B_TZONE,
        P_B_LAT,
        P_B_LNG,
        P_B_PHONE,
        P_B_MOBILE,
        P_B_FAX,
        P_B_INDUSTRY,
        P_B_ORGANIZATION,
        P_B_DEPARTMENT,
        P_B_JOB,
        P_B_MAIL,
        P_B_WEB,
        P_TAGS,
        P_CREATED,
        P_UPDATED
      )
      values (
        id,
        domain_id,
        category_id,
        kind,
        name,
        title,
        fName,
        mName,
        lName,
        fullName,
        gender,
        birthday,
        iri,
        foaf,
        photo,
        interests,
        mail,
        web,
        icq,
        skype,
        aim,
        yahoo,
        msn,
        hCountry,
        hState,
        hCity,
        hCode,
        hAddress1,
        hAddress2,
        hTzone,
        hLat,
        hLng,
        hPhone,
        hMobile,
        hFax,
        hMail,
        hWeb,
        bCountry,
        bState,
        bCity,
        bCode,
        bAddress1,
        bAddress2,
        bTzone,
        bLat,
        bLng,
        bPhone,
        bMobile,
        bFax,
        bIndustry,
        bOrganization,
        bDepartment,
        bJob,
        bMail,
        bWeb,
        tags,
        now (),
        now ()
      );
  } else {
    update AB.WA.PERSONS
       set P_CATEGORY_ID = category_id,
           P_KIND = kind,
           P_NAME = name,
           P_TITLE = title,
           P_FIRST_NAME = fName,
           P_MIDDLE_NAME = mName,
           P_LAST_NAME = lName,
           P_FULL_NAME = fullName,
           P_GENDER = gender,
           P_BIRTHDAY = birthday,
           P_IRI = iri,
           P_FOAF = foaf,
           P_PHOTO = photo,
           P_INTERESTS = interests,
           P_MAIL = mail,
           P_WEB = web,
           P_ICQ = icq,
           P_SKYPE = skype,
           P_AIM = aim,
           P_YAHOO = yahoo,
           P_MSN = msn,
           P_H_ADDRESS1 = hAddress1,
           P_H_ADDRESS2 = hAddress2,
           P_H_CODE = hCode,
           P_H_CITY = hCity,
           P_H_STATE = hState,
           P_H_COUNTRY = hCountry,
           P_H_TZONE = hTzone,
           P_H_LAT = hLat,
           P_H_LNG = hLng,
           P_H_PHONE = hPhone,
           P_H_MOBILE = hMobile,
           P_H_FAX = hFax,
           P_H_MAIL = hMail,
           P_H_WEB = hWeb,
           P_B_ADDRESS1 = bAddress1,
           P_B_ADDRESS2 = bAddress2,
           P_B_CODE = bCode,
           P_B_CITY = bCity,
           P_B_STATE = bState,
           P_B_COUNTRY = bCountry,
           P_B_TZONE = bTzone,
           P_B_LAT = bLat,
           P_B_LNG = bLng,
           P_B_PHONE = bPhone,
           P_B_MOBILE = bMobile,
           P_B_FAX = bFax,
           P_B_INDUSTRY = bIndustry,
           P_B_ORGANIZATION = bOrganization,
           P_B_DEPARTMENT = bDepartment,
           P_B_JOB = bJob,
           P_B_MAIL = bMail,
           P_B_WEB = bWeb,
           P_TAGS = tags,
           P_UPDATED = now()
     where P_ID = id and
           P_DOMAIN_ID = domain_id;
  }
  return id;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_update2 (
  in id integer,
  in domain_id integer,
  in pName varchar,
  in pValue any)
{
  if ((id = -1) and (pName = 'P_NAME'))
  {
    id := sequence_next ('AB.WA.contact_id');
    insert into AB.WA.PERSONS
      (
        P_ID,
        P_DOMAIN_ID,
        P_NAME,
        P_CREATED,
        P_UPDATED
      )
      values
      (
        id,
        domain_id,
        pValue,
        now (),
        now ()
      );
    return id;
  }
  if (id = -1)
    return id;

  if (pName = 'P_KIND')
    update AB.WA.PERSONS set P_KIND = pValue where P_ID = id;
  if (pName = 'P_NAME')
    update AB.WA.PERSONS set P_NAME = pValue where P_ID = id;
  if (pName = 'P_TITLE')
    update AB.WA.PERSONS set P_TITLE = pValue where P_ID = id;
  if (pName = 'P_FIRST_NAME')
    update AB.WA.PERSONS set P_FIRST_NAME = pValue where P_ID = id;
  if (pName = 'P_MIDDLE_NAME')
    update AB.WA.PERSONS set P_MIDDLE_NAME = pValue where P_ID = id;
  if (pName = 'P_LAST_NAME')
    update AB.WA.PERSONS set P_LAST_NAME = pValue where P_ID = id;
  if (pName = 'P_FULL_NAME')
    update AB.WA.PERSONS set P_FULL_NAME = pValue where P_ID = id;
  if (pName = 'P_GENDER')
    update AB.WA.PERSONS set P_GENDER = pValue where P_ID = id;
  if (pName = 'P_BIRTHDAY')
    update AB.WA.PERSONS set P_BIRTHDAY = pValue where P_ID = id;
  if (pName = 'P_FOAF')
    update AB.WA.PERSONS set P_FOAF = pValue where P_ID = id;
  if (pName = 'P_INTERESTS')
    update AB.WA.PERSONS set P_INTERESTS = pValue where P_ID = id;
  if (pName = 'P_RELATIONSHIPS')
    update AB.WA.PERSONS set P_RELATIONSHIPS = pValue where P_ID = id;

  if (pName = 'P_MAIL')
    update AB.WA.PERSONS set P_MAIL = pValue where P_ID = id;
  if (pName = 'P_WEB')
    update AB.WA.PERSONS set P_WEB = pValue where P_ID = id;
  if (pName = 'P_ICQ')
    update AB.WA.PERSONS set P_ICQ = pValue where P_ID = id;
  if (pName = 'P_SKYPE')
    update AB.WA.PERSONS set P_SKYPE = pValue where P_ID = id;
  if (pName = 'P_AIM')
    update AB.WA.PERSONS set P_AIM = pValue where P_ID = id;
  if (pName = 'P_YAHOO')
    update AB.WA.PERSONS set P_YAHOO = pValue where P_ID = id;
  if (pName = 'P_MSN')
    update AB.WA.PERSONS set P_MSN = pValue where P_ID = id;

  if (pName = 'P_H_ADDRESS1')
    update AB.WA.PERSONS set P_H_ADDRESS1 = pValue where P_ID = id;
  if (pName = 'P_H_ADDRESS2')
    update AB.WA.PERSONS set P_H_ADDRESS2 = pValue where P_ID = id;
  if (pName = 'P_H_CODE')
    update AB.WA.PERSONS set P_H_CODE = pValue where P_ID = id;
  if (pName = 'P_H_CITY')
    update AB.WA.PERSONS set P_H_CITY = pValue where P_ID = id;
  if (pName = 'P_H_STATE')
    update AB.WA.PERSONS set P_H_STATE = pValue where P_ID = id;
  if (pName = 'P_H_COUNTRY')
    update AB.WA.PERSONS set P_H_COUNTRY = pValue where P_ID = id;
  if (pName = 'P_H_TZONE')
    update AB.WA.PERSONS set P_H_TZONE = pValue where P_ID = id;
  if (pName = 'P_H_LAT')
    update AB.WA.PERSONS set P_H_LAT = pValue where P_ID = id;
  if (pName = 'P_H_LNG')
    update AB.WA.PERSONS set P_H_LNG = pValue where P_ID = id;
  if (pName = 'P_H_PHONE')
    update AB.WA.PERSONS set P_H_PHONE = pValue where P_ID = id;
  if (pName = 'P_H_MOBILE')
    update AB.WA.PERSONS set P_H_MOBILE = pValue where P_ID = id;
  if (pName = 'P_H_FAX')
    update AB.WA.PERSONS set P_H_FAX = pValue where P_ID = id;
  if (pName = 'P_H_MAIL')
    update AB.WA.PERSONS set P_H_MAIL = pValue where P_ID = id;
  if (pName = 'P_H_WEB')
    update AB.WA.PERSONS set P_H_WEB = pValue where P_ID = id;

  if (pName = 'P_B_ADDRESS1')
    update AB.WA.PERSONS set P_B_ADDRESS1 = pValue where P_ID = id;
  if (pName = 'P_B_ADDRESS2')
    update AB.WA.PERSONS set P_B_ADDRESS2 = pValue where P_ID = id;
  if (pName = 'P_B_CODE')
    update AB.WA.PERSONS set P_B_CODE = pValue where P_ID = id;
  if (pName = 'P_B_CITY')
    update AB.WA.PERSONS set P_B_CITY = pValue where P_ID = id;
  if (pName = 'P_B_STATE')
    update AB.WA.PERSONS set P_B_STATE = pValue where P_ID = id;
  if (pName = 'P_B_COUNTRY')
    update AB.WA.PERSONS set P_B_COUNTRY = pValue where P_ID = id;
  if (pName = 'P_B_TZONE')
    update AB.WA.PERSONS set P_B_TZONE = pValue where P_ID = id;
  if (pName = 'P_B_LAT')
    update AB.WA.PERSONS set P_B_LAT = pValue where P_ID = id;
  if (pName = 'P_B_LNG')
    update AB.WA.PERSONS set P_B_LNG = pValue where P_ID = id;
  if (pName = 'P_B_PHONE')
    update AB.WA.PERSONS set P_B_PHONE = pValue where P_ID = id;
  if (pName = 'P_B_MOBILE')
    update AB.WA.PERSONS set P_B_MOBILE = pValue where P_ID = id;
  if (pName = 'P_B_FAX')
    update AB.WA.PERSONS set P_B_FAX = pValue where P_ID = id;
  if (pName = 'P_B_MAIL')
    update AB.WA.PERSONS set P_B_MAIL = pValue where P_ID = id;
  if (pName = 'P_B_WEB')
    update AB.WA.PERSONS set P_B_WEB = pValue where P_ID = id;
  if (pName = 'P_B_ORGANIZATION')
    update AB.WA.PERSONS set P_B_ORGANIZATION = pValue where P_ID = id;
  if (pName = 'P_B_DEPARTMENT')
    update AB.WA.PERSONS set P_B_DEPARTMENT = pValue where P_ID = id;
  if (pName = 'P_B_JOB')
    update AB.WA.PERSONS set P_B_JOB = pValue where P_ID = id;
  if (pName = 'P_ACL')
    update AB.WA.PERSONS set P_ACL = pValue where P_ID = id;

  update AB.WA.PERSONS set P_UPDATED = now () where P_ID = id;

  return id;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_update3 (
  in id integer,
  in domain_id integer,
  in pFields any,
  in pValues any,
  in tags varchar)
{
  declare N, L varchar;
  declare S varchar;
  declare st, msg, meta, rows any;

  S := '';
  L := length (pFields);
  for (N := 0; N < L; N := N + 1)
  {
    S := S || ', ' || pFields[N] || ' = ?';
  }
  if (trim (tags) <> '')
  {
    S := S || ', P_TAGS = ?';
    pValues := vector_concat (pValues, vector (trim (tags)));
  }
  if (S <> '')
  {
    S := 'update AB.WA.PERSONS set P_UPDATED = now ()' || S || ' where P_ID = ' || cast (id as varchar);
    exec (S, st, msg, pValues, 0, meta, rows);
  }
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_update4 (
  in id integer,
  in domain_id integer,
  in pFields any,
  in pValues any,
  in tags varchar,
  in validation any := null)
{
  declare L, N, M varchar;
  declare S varchar;
  declare st, msg, meta, rows, F, V any;

  if (not isnull (validation) and length (validation))
  {
    S := sprintf ('select P_ID from AB.WA.PERSONS where P_DOMAIN_ID = %d', domain_id);
    V := vector ();
    for (N := 0; N < length (validation); N := N + 1)
    {
      M := AB.WA.vector_index (pFields, validation [N]);
      if (not isnull (M))
      {
        if (not is_empty_or_null (pValues [M]))
        {
          S := S || sprintf (' and %s = ?', pFields [M]);
          V := vector_concat (V, vector (pValues [M]));
        }
      }
    }
    if (length (V) = length (validation))
    {
      st := '00000';
      exec (S, st, msg, V, 0, meta, rows);
      if ((st = '00000') and (length (rows) > 0))
      {
        V := vector ();
        F := vector ();
        for (N := 0; N < length (pFields); N := N + 1) {
          if (not AB.WA.vector_contains (validation, pFields [N]))
          {
            F := vector_concat (F, vector (pFields [N]));
            V := vector_concat (V, vector (pValues [N]));
          }
        }
        pFields := F;
        pValues := V;

        id := vector ();
        for (N := 0; N < length (rows); N := N + 1)
        {
          id := vector_concat (id, vector (rows [N][0]));
      }
    }
  }
  }

  if ((isinteger (id)) and (id = -1))
  {
    L := length (pFields);
    for (N := 0; N < L; N := N + 1)
    {
      if (pFields [N] = 'P_NAME')
      {
        id := sequence_next ('AB.WA.contact_id');
        insert into AB.WA.PERSONS
          (
            P_ID,
            P_DOMAIN_ID,
            P_NAME,
            P_CREATED,
            P_UPDATED
          )
          values
          (
            id,
            domain_id,
            pValues [N],
            now (),
            now ()
          );
      }
      }
  _exit:;
    if (isinteger (id) and (id = -1))
    {
      return 0;
    }
    V := vector ();
    F := vector ();
    for (N := 0; N < L; N := N + 1)
    {
      if ('P_NAME' <> pFields [N])
      {
        F := vector_concat (F, vector (pFields [N]));
        V := vector_concat (V, vector (pValues [N]));
      }
    }
    pFields := F;
    pValues := V;
  }
  if (isinteger (id))
  {
    id := vector (id);
  }
  for (N := 0; N < length (id); N := N + 1)
  {
    AB.WA.contact_update3 (id[N], domain_id, pFields, pValues, tags);
  }

  return id;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_field (
  in id integer,
  in pName varchar)
{
  declare st, msg, meta, rows any;

  st := '00000';
  exec (sprintf ('select %s from AB.WA.PERSONS where P_ID = ?', pName), st, msg, vector (id), 0, meta, rows);
  if ('00000' = st)
    return rows[0][0];
  return null;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_delete (
  in id integer)
{
  delete from AB.WA.PERSONS where P_ID = id;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.person_permissions (
  in id integer,
  in domain_id integer,
  in access_role varchar)
{
  declare person_domain_id integer;

  person_domain_id := (select P_DOMAIN_ID from AB.WA.PERSONS where P_ID = id);
  if (isnull (person_domain_id))
    return '';
  if (person_domain_id = domain_id)
  {
    if (AB.WA.access_is_write (access_role))
      return 'W';
    return 'R';
  }
  return '';
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_tags_select (
  in id integer,
  in domain_id integer)
{
  return coalesce((select P_TAGS from AB.WA.PERSONS where P_ID = id and P_DOMAIN_ID = domain_id), '');
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.contact_tags_update (
  in id integer,
  in domain_id integer,
  in tags any)
{
  update AB.WA.PERSONS
     set P_TAGS = tags,
         P_UPDATED = now()
   where P_ID = id and
         P_DOMAIN_ID = domain_id;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.import_vcard (
  in domain_id integer,
  in content any,
  in options any := null,
  in validation any := null,
  in updatedBefore integer := null,
  in externalUID varchar := null)
{
  declare L, M, N, nLength, mLength, id integer;
  declare tmp, uid, oTags, data, pFields, pValues, pField, pField2 any;
  declare xmlData, xmlItems, itemName, Meta, V any;
  declare vcardImported any;

  vcardImported := vector ();
  oTags := case when isnull (options) then '' else get_keyword ('tags', options, '') end;

  Meta := vector
    (
      'P_UID',            null, 'UID/val',
      'P_NAME',           null, 'NICKNAME/val|N/fld[1]|N/fld[2]|N/val',
      'P_TITLE',          null, 'N/fld[4]',
      'P_FIRST_NAME',     null, 'N/fld[2]',
      'P_MIDDLE_NAME',    null, 'N/fld[3]',
      'P_LAST_NAME',      null, 'N/fld[1]|N/val',
      'P_FULL_NAME',      null, 'FN/val',
      'P_BIRTHDAY',       null, 'BDAY/val',
      'P_B_ORGANIZATION', null, 'ORG/val|ORG/fld[1]',
      'P_B_JOB',          null, 'TITLE/val',
      'P_H_ADDRESS1',     vector ('*', 'P_H_ADDRESS1', 'HOME',     'P_H_ADDRESS1', 'WORK',     'P_B_ADDRESS1'),                'for \044v in ADR/fld[3] return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_H_ADDRESS2',     vector ('*', 'P_H_ADDRESS2', 'HOME',     'P_H_ADDRESS2', 'WORK',     'P_B_ADDRESS2'),                'for \044v in ADR/fld[2] return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_H_CITY',         vector ('*', 'P_H_CITY',     'HOME',     'P_H_CITY',     'WORK',     'P_B_CITY'),                    'for \044v in ADR/fld[4] return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_H_CODE',         vector ('*', 'P_H_CODE',     'HOME',     'P_H_CODE',     'WORK',     'P_B_CODE'),                    'for \044v in ADR/fld[6] return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_H_STATE',        vector ('*', 'P_H_STATE',    'HOME',     'P_H_STATE',    'WORK',     'P_B_STATE'),                   'for \044v in ADR/fld[5] return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_H_COUNTRY',      vector ('*', 'P_H_COUNTRY',  'HOME',     'P_H_COUNTRY',  'WORK',     'P_B_COUNTRY'),                 'for \044v in ADR/fld[7] return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_MAIL',           vector ('*', 'P_MAIL',       'HOME',     'P_H_MAIL',     'WORK',     'P_B_MAIL',  'PREF', 'P_MAIL'), 'for \044v in EMAIL/val return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_H_PHONE',        vector ('*', 'P_H_PHONE',    'HOME,FAX', 'P_H_FAX',      'WORK,FAX', 'P_B_FAX',   'FAX',  'P_H_FAX', 'HOME', 'P_H_PHONE', 'WORK', 'P_B_PHONE', 'CELL', 'P_H_MOBILE'), 'for \044v in TEL/val return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))',
      'P_WEB',            vector ('*', 'P_WEB',        'HOME',     'P_H_WEB',      'WORK',     'P_B_WEB'),                     'for \044v in URL/val return concat (\044v, for \044t in \044v/../TYPE return concat (" @TYPE_", \044t))'
    );
  mLength := length (Meta);

  -- using DAV parser
  if (not isstring (content))
    content := cast (content as varchar);
    xmlData := DB.DBA.IMC_TO_XML (content);

  xmlData := xml_tree_doc (xmlData);
  xmlItems := xpath_eval ('/*', xmlData, 0);
  foreach (any xmlItem in xmlItems) do
  {
    itemName := xpath_eval ('name(.)', xmlItem);
    if (itemName = 'IMC-VCARD')
    {
      id := -1;
      uid := null;
      pFields := vector ();
      pValues := vector ();
      for (N := 0; N < mLength; N := N + 3)
      {
        pField := Meta [N];
        tmp := xquery_eval (Meta [N+2], xmlItem, 0);
        foreach (any T in tmp) do
        {
          T := cast (T as varchar);
          if (not is_empty_or_null (T))
          {
            pField2 := pField;
            if (pField2 = 'P_BIRTHDAY')
            {
              {
                declare continue handler for sqlstate '*'
              {
                  T := '';
                };
                T := AB.WA.dt_reformat (T, 'YMD');
              }
            }
            else if (pField2 = 'P_UID')
            {
              uid := T;
            }
            if (not is_empty_or_null (T))
            {
              if (not isnull (Meta [N+1]))
              {
                if (strstr (T, ' @TYPE_') <> 0)
                {
                  pField2 := '';
                  for (M := 0; M < length (Meta [N+1]); M := M + 2)
                  {
                    if ((Meta [N+1][M] = '*') and isnull (strstr (T, ' @TYPE_')))
                    {
                      pField2 := Meta [N+1][M+1];
            } else {
                      V := split_and_decode (Meta [N+1][M], 0, '\0\0,');
                      for (L := 0; L < length (V); L := L + 1)
                        if (isnull (strstr (T, ' @TYPE_' || V[L])))
                          goto _exit;
                      pField2 := Meta [N+1][M+1];
                    _exit:;
                    }
                  }
                  M := strstr (T, ' @TYPE_');
                  if (not isnull (M))
                    T := subseq (T, 0, M);
                }
              }
              if (not AB.WA.vector_contains (pFields, pField2))
              {
                  pFields := vector_concat (pFields, vector (pField2));
              pValues := vector_concat (pValues, vector (T));
            }
          }
        }
      }
      }
      if (isnull (uid) and not isnull (externalUID))
      {
        N := strchr (externalUID, '_');
        if (isnull (N))
        {
          N := 0;
        }
        tmp := subseq (externalUID, N, length (externalUID));
        id := coalesce ((select P_ID from AB.WA.PERSONS where P_DOMAIN_ID = domain_id and P_UID = tmp), -1);
        if (id <> -1)
        {
          uid := tmp;
        }
      }
      id := coalesce ((select P_ID from AB.WA.PERSONS where P_DOMAIN_ID = domain_id and P_UID = uid), -1);
      if ((id <> -1) and not isnull (updatedBefore))
      {
        if (exists (select 1 from AB.WA.PERSONS where P_ID = id and P_UPDATED >= updatedBefore))
          goto _skip;
      }
      commit work;
      connection_set ('__addressbook_import', '1');
      id := AB.WA.contact_update4 (id, domain_id, pFields, pValues, oTags, validation);
      connection_set ('__addressbook_import', '0');
      vcardImported := vector_concat (vcardImported, id);

    _skip:;
    }
  }
  return vcardImported;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.import_vcard2 (
  in domain_id integer,
  in content any,
  in options any := null,
  in validation any := null,
  in contentType any := 0,
  in contentIRI varchar := null)
{
  declare N, M, L, mLength, iLength, id integer;
  declare tmp, tmp2, tmpTags, data, pFields, pValues any;
  declare Tags, Meta, Persons, Person, Items any;
  declare S, T, name, fullName varchar;

  Tags := case when isnull (options) then '' else get_keyword ('tags', options, '') end;
  if (isnull (contentIRI))
    contentIRI := AB.WA.ab_graph_create ();

  declare exit handler for sqlstate '*'
  {
    dbg_obj_print (__SQL_STATE, __SQL_MESSAGE);
    AB.WA.ab_graph_delete (contentIRI);
    signal ('TEST', 'Bad import source!<>');
  };

  Meta := vector
    (
      'P_NAME',
      'P_FIRST_NAME',
      'P_LAST_NAME',
      'P_BIRTHDAY',
      'P_TITLE',
      'P_TAGS'
    );
  mLength := length (Meta);

  if (contentType = 0)
  {
    DB.DBA.RDF_LOAD_RDFXML (content, contentIRI, contentIRI);
  } else {
    declare st, msg, meta any;

    -- S := sprintf ('SPARQL load <%s> into graph <%s>', content, contentIRI);
    S := sprintf ('SPARQL\ndefine get:soft "soft"\n  define get:uri "%s"\nSELECT *\n  FROM <%s>\n WHERE { ?s ?p ?o }', content, contentIRI);
    st := '00000';
    exec (S, st, msg, vector (), 0, meta, Items);
    if ('00000' <> st)
      signal (st, msg);
  }
  S := ' SPARQL '                                                    ||
       ' define input:storage "" '                                   ||
       ' prefix vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> '     ||
       ' select ?P_ID'                                               ||
       '   from <%s> '                                               ||
       '  where { ?P_ID a vcard:vCard . }';
  Items := AB.WA.ab_sparql (sprintf (S, contentIRI));
  for (L := 0; L < length (Items); L := L + 1)
  {
    AB.WA.ab_sparql (sprintf ('SPARQL\ndefine get:soft "soft"\n  define get:uri "%s"\nSELECT *\n  FROM <%s>\n WHERE { ?s ?p ?o }', Items[L][0], contentIRI));
    S := ' SPARQL ' ||
         ' define input:storage "" ' ||
         ' prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ' ||
         ' prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> '      ||
         ' prefix foaf: <http://xmlns.com/foaf/0.1/> '                 ||
         ' prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> '    ||
         ' prefix vcard: <http://www.w3.org/2001/vcard-rdf/3.0#> '     ||
         ' prefix bio: <http://vocab.org/bio/0.1/> '                   ||
         ' select ?P_NAME ?P_FIRST_NAME ?P_LAST_NAME ?P_BIRTHDAY ?P_TITLE ?P_TAGS' ||
         '   from <%s> '                                          ||
         '  where { '                                             ||
         '         <%s> a vcard:vCard . '                         ||
         '         optional { ?P_ID vcard:NICKNAME ?P_NAME. }. '  ||
         '         optional { ?P_ID vcard:N ?N. '                 ||
         '                    ?N vcard:Given ?P_FIRST_NAME. '     ||
         '                    ?N vcard:Family ?P_LAST_NAME. '     ||
         '                    ?N vcard:Prefix ?P_TITLE. '         ||
         '                  }.'                                   ||
         '         optional { ?P_ID vcard:BDAY ?P_BIRTHDAY} . '   ||
         '         optional { ?P_ID vcard:CATEGORIES ?P_TAGS} . ' ||
         '       }';
    Person := AB.WA.ab_sparql (sprintf (S, Items[L][0], contentIRI));
    if (length (Person) = 1)
    {
      Person := Persons[0];
      name := Person[1];
      if (not is_empty_or_null (name))
      {
        pFields := vector ('P_NAME');
        pValues := vector (name);
        if (content like 'http://%')
        {
          pFields := vector_concat (pFields, vector ('P_FOAF'));
          pValues := vector_concat (pValues, vector (content));
        }
        for (M := 2; M < mLength; M := M + 1)
        {
          tmp := Meta[M];
          tmp2 := Person[M];
          tmpTags := tags;
          if (tmp = 'P_BIRTHDAY')
          {
            {
              declare continue handler for sqlstate '*'
              {
                tmp := '';
              };
              tmp2 := AB.WA.dt_reformat (tmp2, 'Y-M-D');
            }
          }
          if (tmp = 'P_KIND')
          {
            tmp2 := case when (tmp2 = 'http://xmlns.com/foaf/0.1/Organization') then 1 else 0 end;
          }
          if (tmp = 'P_MAIL')
          {
            tmp2 := replace (tmp2, 'mailto:', '');
          }
          if (tmp = 'P_PHONE')
          {
            tmp2 := replace (tmp2, 'tel:', '');
          }
          if (tmp = 'P_TITLE')
          {
            tmp2 := AB.WA.import_title (tmp2);
          }
          if (tmp = 'P_TAGS')
          {
            tmpTags := AB.WA.tags_join (tmpTags, tmp2);
            tmp := '';
          }
          if (tmp <> '')
          {
            pFields := vector_concat (pFields, vector (tmp));
            pValues := vector_concat (pValues, vector (tmp2));
          }
        }
        commit work;
        connection_set ('__addressbook_import', '1');
        AB.WA.contact_update4 (-1, domain_id, pFields, pValues, tmpTags, validation);
        connection_set ('__addressbook_import', '0');
      }
    }
  }

_delete:;
  AB.WA.ab_graph_delete (contentIRI);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.import_title (
  in title varchar)
{
  declare M integer;
  declare V any;

  V := vector ('Mr', 'Mrs', 'Dr', 'Ms', 'Sir');
  for (M := 0; M < length (V); M := M + 1)
  {
    if (lcase (title) like (lcase (V[M])|| '%'))
    {
      return V[M];
    }
  }
  return '';
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.import_foaf (
  inout domain_id integer,
  inout content any,
  in tags any,
  in validation any,
  in contentType any := 0,
  in contentIRI varchar := null,
  in contentItems any := null,
  in contentDepth any := 0,
  in contentLimit any := 100,
  in contentFollow any := 'foaf:knows')
{
  declare N, M, pLength, mLength, iLength, id integer;
  declare tmp, tmp2, tmpTags, data, pFields, pValues any;
  declare Meta, Persons, Person, Items any;
  declare S, T, P, name, fullName varchar;

  if (isnull (contentIRI))
    contentIRI := AB.WA.ab_graph_create ();

  declare exit handler for sqlstate '*'
  {
    -- dbg_obj_print (__SQL_STATE, __SQL_MESSAGE);
    AB.WA.ab_graph_delete (contentIRI);
    signal ('TEST', 'Bad import source!<>');    
  };

  Meta := vector
    (
      'P_ID',
      'P_NAME',
      'P_FULL_NAME',
      'P_KIND',
      'P_FIRST_NAME',
      'P_LAST_NAME',
      'P_BIRTHDAY',
      'P_MAIL',
      'P_WEB',
      'P_ICQ',
      'P_MSN',
      'P_AIM',
      'P_YAHOO',
      'P_TITLE',
      'P_H_PHONE',
      'P_H_WEB',
      'P_B_WEB',
      'P_H_LAT',
      'P_H_LNG',
      'P_TAGS',
      'P_PHOTO'
    );
  mLength := length (Meta);

  if (contentType = 0)
  {
    DB.DBA.RDF_LOAD_RDFXML (content, contentIRI, contentIRI);
  } else {
    declare st, msg, meta any;
  
    T := case when contentDepth then sprintf ('  define input:grab-depth %d\n  define input:grab-limit %d\n  define input:grab-seealso <%s>\n  define input:grab-destination <%s>\n', contentDepth, contentLimit, contentFollow, contentIRI) else '' end;
    S := sprintf ('SPARQL\n%s  define get:soft "soft"\n  define get:uri "%s"\nSELECT *\n  FROM <%s>\n WHERE { ?s ?p ?o }', T, content, contentIRI);
    st := '00000';
    exec (S, st, msg, vector (), 0, meta, Items);
    if ('00000' <> st)
      signal (st, msg);
  }
  if (isnull (contentItems))
  {
    Items := AB.WA.ab_sparql (sprintf (' SPARQL                                                    \n' ||
                                       ' define input:storage ""                                   \n' ||
                                       ' PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n' ||
                                       ' PREFIX foaf: <http://xmlns.com/foaf/0.1/>                 \n' ||
                                       ' SELECT ?x                                                 \n' ||
                                       '   FROM <%s>                                               \n' ||
                                       '  WHERE {                                                  \n' ||
                                       '          {?x a foaf:Person .}                             \n' ||
                                       '          UNION                                            \n' ||
                                       '          {?x a foaf:Organization .}                       \n' ||
                                       '        }', contentIRI));
  } else {
    Items := contentItems;
  }
  iLength := length (Items);

    S := ' SPARQL ' ||
       ' define input:storage "" ' ||
       ' prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ' ||
       ' prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ' ||
       ' prefix foaf: <http://xmlns.com/foaf/0.1/> ' ||
       ' prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> ' ||
       ' prefix bio: <http://vocab.org/bio/0.1/> ' ||
       ' select ?P_ID, ?P_NAME, ?P_FULL_NAME, ?P_KIND, ?P_FIRST_NAME, ?P_LAST_NAME, ?P_BIRTHDAY, ?P_MAIL, ?P_WEB, ?P_ICQ, ?P_MSN, ?P_AIM, ?P_YAHOO, ?P_TITLE, ?P_H_PHONE, ?P_H_WEB, ?P_B_WEB, ?P_H_LAT, ?P_H_LNG, ?P_TAGS, ?P_PHOTO' ||
       '   from <%s> ' ||
       '  where { ' ||
       '         {?P_ID a foaf:Person } UNION {?P_ID a foaf:Organization } . ' ||
       '         ?P_ID rdf:type ?P_KIND .' ||
       '         optional { ?P_ID foaf:nick ?P_NAME} . '                        ||
       '         optional { ?P_ID foaf:title ?P_TITLE} . '                      ||
       '         optional { ?P_ID foaf:name ?P_FULL_NAME} . '                   ||
       '         optional { ?P_ID foaf:firstNname ?P_FIRST_NAME} . '            ||
       '         optional { ?P_ID foaf:family_name ?P_LAST_NAME} . '            ||
       '         optional { ?P_ID foaf:dateOfBirth ?P_BIRTHDAY} . '             ||
       '         optional { ?P_ID foaf:mbox ?P_MAIL} . '                        ||
       '         optional { ?P_ID foaf:workplaceHomepage ?P_WEB} . '            ||
       '         optional { ?P_ID foaf:icqChatID ?P_ICQ } .'                    ||
       '         optional { ?P_ID foaf:msnChatID ?P_MSN } .'                    ||
       '         optional { ?P_ID foaf:aimChatID ?P_AIM } .'                    ||
       '         optional { ?P_ID foaf:yahooChatID ?P_YAHOO } .'                ||
       '         optional { ?P_ID foaf:phone ?P_H_PHONE } .'                    ||
       '         optional { ?P_ID foaf:homepage ?P_H_WEB} . '                   ||
       '         optional{ ?P_ID foaf:workplaceHomepage ?P_B_WEB } .'          ||
       '         optional{ ?P_ID foaf:depiction ?P_PHOTO } .'                  ||
       '         optional{ ?P_ID foaf:based_near ?based_near .'                ||
       '                   ?based_near geo:lat ?P_H_LAT ;'                     ||
       '                               geo:long ?P_H_LNG .'                    ||
       '                 } .'                                                  ||
       '         optional{ ?P_ID bio:keywords ?P_TAGS } .'                     ||
       '         optional{ ?P_ID foaf:interest ?interest .'                    ||
       '                   ?interest rdfs:label ?interest_label. } .'          ||
         '       }';
  Persons := AB.WA.ab_sparql (sprintf (S, contentIRI));
  pLength := length (Persons);
  P := '';
  for (N := 0; N < pLength; N := N + 1)
    {
    Person := Persons[N];
    if (P <> Person[0])
      {
      for (M := 0; M < iLength; M := M + 1)
      {
        if (Person[0] = Items[M][0])
      {
          goto _import;
        }
      }
      }
    goto _next;

  _import:;
    P := Person[0];
    name := Person[1];
    fullName := Person[2];
    if (isnull (fullName) and not (isnull (Person[4]) and isnull (Person[5])))
    {
      fullName := trim (Person[4] || ' ' || Person[5]);
    }
    if (not is_empty_or_null (coalesce (name, fullName)))
      {
        pFields := vector ('P_NAME');
      pValues := vector (coalesce (name, fullName));
      if (P not like 'nodeID://%')
	      {
          pFields := vector_concat (pFields, vector ('P_IRI'));
        pValues := vector_concat (pValues, vector (P));
	      }
	      if (content like 'http://%')
	      {
          pFields := vector_concat (pFields, vector ('P_FOAF'));
          pValues := vector_concat (pValues, vector (content));
	      }
            if (not isnull (fullName))
            {
        pFields := vector_concat (pFields, vector ('P_FULL_NAME'));
              pValues := vector_concat (pValues, vector (fullName));
            }
      for (M := 3; M < mLength; M := M + 1)
      {
            tmp := Meta[M];
        tmp2 := Person[M];
        tmpTags := tags;
            if (tmp = 'P_BIRTHDAY')
            {
              {
            declare continue handler for sqlstate '*'
            {
                  tmp := '';
                };
                tmp2 := AB.WA.dt_reformat (tmp2, 'Y-M-D');
              }
            }
            if (tmp = 'P_KIND')
            {
          tmp2 := case when (tmp2 = 'http://xmlns.com/foaf/0.1/Organization') then 1 else 0 end;
            }
            if (tmp = 'P_MAIL')
            {
              tmp2 := replace (tmp2, 'mailto:', '');
            }
        if (tmp = 'P_PHONE')
        {
          tmp2 := replace (tmp2, 'tel:', '');
        }
        if (tmp = 'P_TITLE')
        {
          tmp2 := AB.WA.import_title (tmp2);
        }
        if (tmp = 'P_TAGS')
        {
          tmpTags := AB.WA.tags_join (tmpTags, tmp2);
          tmp := '';
        }
            if (tmp <> '')
            {
              pFields := vector_concat (pFields, vector (tmp));
              pValues := vector_concat (pValues, vector (tmp2));
            }
          }
      commit work;
      connection_set ('__addressbook_import', '1');
      AB.WA.contact_update4 (-1, domain_id, pFields, pValues, tmpTags, validation);
      connection_set ('__addressbook_import', '0');
      }
  _next:;
  }

_delete:;
  AB.WA.ab_graph_delete (contentIRI);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.import_foaf_content (
  inout content any,
  in contentType any := 0,
  in contentIRI any := null,
  in contentDepth any := 0,
  in contentLimit any := 100,
  in contentFollow any := 'foaf:knows')
{
  declare N, M integer;
  declare tmp, Items, Persons any;
  declare S, T, personIRI varchar;

  declare exit handler for sqlstate '*'
  {
    Persons := vector ();
    AB.WA.ab_graph_delete (contentIRI);
    goto _exit;
  };

  Persons := vector ();
  if (isnull (contentIRI))
    contentIRI := AB.WA.ab_graph_create ();
  AB.WA.ab_graph_delete (contentIRI);

  -- store in QUAD Store
  if (contentType)
  {
    declare st, msg, meta any;

    T := '';
    if (contentDepth)
      T := sprintf ('  define input:grab-depth %d\n  define input:grab-limit %d\n  define input:grab-seealso <%s>\n  define input:grab-destination <%s>\n', contentDepth, contentLimit, contentFollow, contentIRI);
    S := sprintf ('sparql \n%s  define get:soft "soft"\n  define get:uri "%s"\nSELECT *\n  FROM <%s>\n WHERE { ?s ?p ?o }', T, content, contentIRI);
    st := '00000';
    exec (S, st, msg, vector (), 0, meta, Items);
    if ('00000' <> st)
      signal (st, msg);
  } else {
    DB.DBA.RDF_LOAD_RDFXML (content, contentIRI, contentIRI);
  }
  Items := AB.WA.ab_sparql (sprintf (' sparql \n' ||
                                     ' define input:storage ""                                   \n' ||
                                     ' prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n' ||
                                     ' prefix foaf: <http://xmlns.com/foaf/0.1/>                 \n' ||
                                     ' select ?person, ?nick, ?name, ?mbox                       \n' ||
                                     '   from <%s>                                               \n' ||
                                     '  where {                                                  \n' ||
                                     '          [] a foaf:PersonalProfileDocument ;              \n' ||
                                     '             foaf:primaryTopic ?person .                   \n' ||
                                     '          optional { ?person foaf:nick ?nick } .           \n' ||
                                     '          optional { ?person foaf:name ?name } .           \n' ||
                                     '          optional { ?person foaf:mbox ?mbox } .           \n' ||
                                     '        }', contentIRI));
  if (length (Items))
  {
    personIRI := Items[0][0];
    tmp := replace (Items[N][3], 'mailto:', '');
    Persons := vector_concat (Persons, vector (vector (1, personIRI,  coalesce (Items[N][2], Items[N][1]), tmp)));
    Items := AB.WA.ab_sparql (sprintf (' SPARQL                                                    \n' ||
                                       ' define input:storage ""                                   \n' ||
                                       ' prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n' ||
                                       ' prefix foaf: <http://xmlns.com/foaf/0.1/>                 \n' ||
                                       ' select ?person, ?nick, ?name, ?mbox                       \n' ||
                                       '   from <%s>                                               \n' ||
                                       '  where {                                                  \n' ||
                                       '          {                                                \n' ||
                                       '            <%s> foaf:knows ?person .                      \n' ||
                                       '            ?person a foaf:Person .                        \n' ||
                                       '            optional { ?person foaf:nick ?nick } .         \n' ||
                                       '            optional { ?person foaf:name ?name } .         \n' ||
                                       '            optional { ?person foaf:mbox ?mbox } .         \n' ||
                                       '          }                                                \n' ||
                                       '          UNION                                            \n' ||
                                       '          {                                                \n' ||
                                       '            <%s> foaf:knows ?person .                      \n' ||
                                       '            ?person a foaf:Organization .                  \n' ||
                                       '            optional { ?person foaf:nick ?nick } .         \n' ||
                                       '            optional { ?person foaf:name ?name } .         \n' ||
                                       '            optional { ?person foaf:mbox ?mbox } .         \n' ||
                                       '          }                                                \n' ||
                                       '        }', contentIRI, personIRI, personIRI));
  } else {
    Items := AB.WA.ab_sparql (sprintf (' SPARQL                                                    \n' ||
                                       ' define input:storage ""                                   \n' ||
                                       ' prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n' ||
                                       ' prefix foaf: <http://xmlns.com/foaf/0.1/>                 \n' ||
                                       ' select ?person, ?nick, ?name, ?mbox                       \n' ||
                                       '   from <%s>                                               \n' ||
                                       '  where {                                                  \n' ||
                                       '          {                                                \n' ||
                                       '            ?person a foaf:Person .                        \n' ||
                                       '            optional { ?person foaf:nick ?nick } .         \n' ||
                                       '            optional { ?person foaf:name ?name } .         \n' ||
                                       '            optional { ?person foaf:mbox ?mbox } .         \n' ||
                                       '          }                                                \n' ||
                                       '          UNION                                            \n' ||
                                       '          {                                                \n' ||
                                       '            ?person a foaf:Organization .                  \n' ||
                                       '            optional { ?person foaf:nick ?nick } .         \n' ||
                                       '            optional { ?person foaf:name ?name } .         \n' ||
                                       '            optional { ?person foaf:mbox ?mbox } .         \n' ||
                                       '          }                                                \n' ||
                                       '        }', contentIRI));
  }
  for (N := 0; N < length (Items); N := N + 1)
  {
    if (not isnull (coalesce (Items[N][2], Items[N][1])))
    {
      for (M := 0; M < length (Persons); M := M + 1)
      {
        if (Persons[M][1] = Items[N][0])
          goto _skip;
      }
      tmp := replace (Items[N][3], 'mailto:', '');
      Persons := vector_concat (Persons, vector (vector (0, Items[N][0], coalesce (Items[N][2], Items[N][1]), tmp)));
    _skip:;
    }
  }

_exit:;
  return Persons;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.import_csv (
  in domain_id integer,
  in content any,
  in tags any,
  in maps any,
  in validation any)
{
  declare N, M, nLength, mLength, id integer;
  declare tmp, tmp2, data, pFields, pValues any;
  declare nameIdx, firstNameIdx, lastNameIdx, fullNameIdx integer;
  declare name, fullName varchar;

  nameIdx := -1;
  firstNameIdx := -1;
  lastNameIdx := -1;
  fullNameIdx := -1;
  for (N := 0; N < length (maps); N := N + 2)
  {
    if (maps[N+1] = 'P_NAME')
      nameIdx := cast (maps[N] as integer);
    if (maps[N+1] = 'P_FIRST_NAME')
      firstNameIdx := cast (maps[N] as integer);
    if (maps[N+1] = 'P_LAST_NAME')
      lastNameIdx := cast (maps[N] as integer);
    if (maps[N+1] = 'P_FULL_NAME')
      fullNameIdx := cast (maps[N] as integer);
  }
  nLength := length (content);
  for (N := 1; N < nLength; N := N + 1)
  {
    data := split_and_decode (content [N], 0, '\0\0,');
    name := '';
    fullName := '';
    if ((nameIdx <> -1) and (nameIdx < length (data)))
      name := trim (trim (data[nameIdx], '"'));
    if ((fullNameIdx <> -1) and (fullNameIdx < length (data)))
      fullName := trim (trim (data[fullNameIdx], '"'));
    if (fullName = '')
    {
      if ((firstNameIdx <> -1) and (firstNameIdx < length (data)))
         fullName := trim (trim (data[firstNameIdx], '"'));
      if ((lastNameIdx <> -1) and (lastNameIdx < length (data)))
         fullName := fullName || ' ' || trim (trim (data[lastNameIdx], '"'));
       fullName := trim (fullName);
    }
    if (name = '')
      name := fullName;
    if (name <> '')
    {
      pFields := vector ('P_NAME');
      pValues := vector (name);
      mLength := length (data);
      for (M := 0; M < mLength; M := M + 1)
      {
        if (M <> nameIdx)
        {
          if (M = fullNameIdx)
          {
            if (fullName <> '')
            {
               pFields := vector_concat (pFields, vector ('P_FULL_NAME'));
               pValues := vector_concat (pValues, vector (fullName));
             }
          } else
          {
             tmp := get_keyword (cast (M as varchar), maps, '');
            if (tmp <> '')
            {
               tmp2 := trim (data[M], '"');
              if (tmp = 'P_BIRTHDAY')
              {
                 {
                   declare continue handler for sqlstate '*' {
                     tmp := '';
                   };
                   tmp2 := AB.WA.dt_reformat (tmp2);
                 }
               }
              if (tmp = 'P_TITLE')
              {
                tmp2 := AB.WA.import_title (tmp2);
              }
              if (tmp <> '')
              {
                 pFields := vector_concat (pFields, vector (tmp));
                 pValues := vector_concat (pValues, vector (tmp2));
               }
             }
           }
         }
      }
      commit work;
      connection_set ('__addressbook_import', '1');
      AB.WA.contact_update4 (-1, domain_id, pFields, pValues, tags, validation);
      connection_set ('__addressbook_import', '0');
    }
  }
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.import_ldap (
  in domain_id integer,
  in content any,
  in tags any,
  in maps any,
  in validation any)
{
  declare N, M, nLength, mLength, id integer;
  declare data, pFields, pValues any;

  nLength := length (content);
  for (N := 0; N < nLength; N := N + 2)
  {
    if (content [N] = 'entry')
    {
      data := content [N+1];
      mLength := length (data);
        pFields := vector ();
        pValues := vector ();
      for (M := 0; M < mLength; M := M + 2)
      {
        if (get_keyword (data[M], maps, '') <> '')
        {
            pFields := vector_concat (pFields, vector (get_keyword (data[M], maps)));
            pValues := vector_concat (pValues, vector (case when isstring (data[M+1]) then data[M+1] else data[M+1][0] end));
          }
        }
      commit work;
      connection_set ('__addressbook_import', '1');
      AB.WA.contact_update4 (-1, domain_id, pFields, pValues, tags, validation);
      connection_set ('__addressbook_import', '0');
    }
  }
}
;

----------------------------------------------------------------------
--
create procedure AB.WA.export_vcard_line (
  in property varchar,
  in value any,
  inout sStream any)
{
  if (not is_empty_or_null (value))
  {
    http (sprintf ('%s:%s\r\n', property, cast (value as varchar)), sStream);
  }
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_vcard (
  in domain_id integer,
  in entries any := null,
  in options any := null)
{
  declare S varchar;
  declare oTagsInclude, oTagsExclude any;
  declare sStream any;

  oTagsInclude := null;
  oTagsExclude := null;
  if (not isnull (options))
  {
    oTagsInclude := get_keyword ('tagsInclude', options);
    oTagsExclude := get_keyword ('tagsExclude', options);
  }
  sStream := string_output();
  for (select * from AB.WA.PERSONS where P_DOMAIN_ID = domain_id  and (entries is null or AB.WA.vector_contains (entries, P_ID))) do
  {
    if (AB.WA.tags_exchangeTest (P_TAGS, oTagsInclude, oTagsExclude))
  {
	  http ('BEGIN:VCARD\r\n', sStream);
	  http ('VERSION:2.1\r\n', sStream);

      AB.WA.export_vcard_line ('REV', AB.WA.dt_iso8601 (P_UPDATED), sStream);

	  -- personal
      AB.WA.export_vcard_line ('UID', P_UID, sStream);
      AB.WA.export_vcard_line ('NICKNAME', P_NAME, sStream);
      AB.WA.export_vcard_line ('FN', P_FULL_NAME, sStream);

    -- Home
	  S := coalesce (P_LAST_NAME, '');
	  S := S || ';' || coalesce (P_FIRST_NAME, '');
	  S := S || ';' || coalesce (P_MIDDLE_NAME, '');
	  S := S || ';' || coalesce (P_TITLE, '');
	  if (S <> ';;;')
  	  {
        AB.WA.export_vcard_line ('N', S, sStream);
      }
      AB.WA.export_vcard_line ('BDAY', AB.WA.dt_format (P_BIRTHDAY, 'Y-M-D'), sStream);

	  -- mail
      AB.WA.export_vcard_line ('EMAIL;TYPE=PREF;TYPE=INTERNET', P_MAIL, sStream);
	  -- web
      AB.WA.export_vcard_line ('URL', P_WEB, sStream);
      AB.WA.export_vcard_line ('URL;TYPE=HOME', P_H_WEB, sStream);
      AB.WA.export_vcard_line ('URL;TYPE=WORK', P_B_WEB, sStream);
    -- Home
	  S := ';';
	  S := S || ''  || coalesce (P_H_ADDRESS1, '');
	  S := S || ';' || coalesce (P_H_ADDRESS2, '');
	  S := S || ';' || coalesce (P_H_CITY, '');
	  S := S || ';' || coalesce (P_H_STATE, '');
	  S := S || ';' || coalesce (P_H_CODE, '');
	  S := S || ';' || coalesce (P_H_COUNTRY, '');
	  if (S <> ';;;;;;')
  	  {
  	    AB.WA.export_vcard_line ('ADR;TYPE=HOME', S, sStream);
  	  }
      AB.WA.export_vcard_line ('TS', P_H_TZONE, sStream);
      AB.WA.export_vcard_line ('TEL;TYPE=HOME', P_H_PHONE, sStream);
      AB.WA.export_vcard_line ('TEL;TYPE=HOME;TYPE=CELL', P_H_MOBILE, sStream);

    -- Business
	  S := ';';
	  S := S || ''  || coalesce (P_B_ADDRESS1, '');
	  S := S || ';' || coalesce (P_B_ADDRESS2, '');
	  S := S || ';' || coalesce (P_B_CITY, '');
	  S := S || ';' || coalesce (P_B_STATE, '');
	  S := S || ';' || coalesce (P_B_CODE, '');
	  S := S || ';' || coalesce (P_B_COUNTRY, '');
	  if (S <> ';;;;;;')
  	  {
    	  AB.WA.export_vcard_line ('ADR;TYPE=WORK', S, sStream);
    	}
      AB.WA.export_vcard_line ('TEL;TYPE=WORK', P_B_PHONE, sStream);
      AB.WA.export_vcard_line ('TEL;TYPE=WORK;TYPE=CELL', P_B_MOBILE, sStream);

      AB.WA.export_vcard_line ('ORG', P_B_ORGANIZATION, sStream);
      AB.WA.export_vcard_line ('TITLE', P_B_JOB, sStream);

	  http ('END:VCARD\r\n', sStream);
	}
	}
  return string_output_string(sStream);
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_csv_head ()
{
  return '"Name",' ||
         '"Title",' ||
         '"First Name",' ||
         '"Last Name",' ||
         '"Full Name",' ||
         '"Gender",' ||
         '"Birthday",' ||
         '"Mail",' ||
         '"Web Page",' ||
         '"Home Address",' ||
         '"Home Address 2",' ||
         '"Home City",' ||
         '"Home State",' ||
         '"Home Postal Code",' ||
         '"Home Country",' ||
         '"Home Timezone",' ||
         '"Home Phone",' ||
         '"Home Mobile",' ||
         '"Home Mail",' ||
         '"Home Web Page",' ||
         '"Business Address",' ||
         '"Business Address 2",' ||
         '"Business City",' ||
         '"Business State",' ||
         '"Business Postal Code",' ||
         '"Business Country",' ||
         '"Business Timezone",' ||
         '"Business Phone",' ||
         '"Business Mobile",' ||
         '"Business Mail",' ||
         '"Business Web Page",' ||
         '"Industry",' ||
         '"Company",' ||
         '"Job Title",' ||
         '"Tags"\r\n';
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_csv (
  in domain_id integer,
  in entries any := null)
{
  declare S varchar;

  S := '';
  for (select * from AB.WA.PERSONS where P_DOMAIN_ID = domain_id  and (entries is null or AB.WA.vector_contains (entries, P_ID))) do
  {
    S := S ||
         sprintf
           (
            '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\r\n',
            coalesce (P_NAME, ''),
            coalesce (P_TITLE, ''),
            coalesce (P_FIRST_NAME, ''),
            coalesce (P_LAST_NAME, ''),
            coalesce (P_FULL_NAME, ''),
            coalesce (P_GENDER, ''),
            case when (isnull (P_BIRTHDAY)) then '' else AB.WA.dt_format (P_BIRTHDAY, 'Y-M-D') end,
            coalesce (P_MAIL, ''),
            coalesce (P_WEB, ''),
            coalesce (P_H_ADDRESS1, ''),
            coalesce (P_H_ADDRESS2, ''),
            coalesce (P_H_CITY, ''),
            coalesce (P_H_STATE, ''),
            coalesce (P_H_CODE, ''),
            coalesce (P_H_COUNTRY, ''),
            coalesce (P_H_TZONE, ''),
            coalesce (P_H_PHONE, ''),
            coalesce (P_H_MOBILE, ''),
            coalesce (P_H_MAIL, ''),
            coalesce (P_H_WEB, ''),
            coalesce (P_B_ADDRESS1, ''),
            coalesce (P_B_ADDRESS2, ''),
            coalesce (P_B_CITY, ''),
            coalesce (P_B_STATE, ''),
            coalesce (P_B_CODE, ''),
            coalesce (P_B_COUNTRY, ''),
            coalesce (P_B_TZONE, ''),
            coalesce (P_B_PHONE, ''),
            coalesce (P_B_MOBILE, ''),
            coalesce (P_B_MAIL, ''),
            coalesce (P_B_WEB, ''),
            coalesce (P_B_INDUSTRY, ''),
            coalesce (P_B_ORGANIZATION, ''),
            coalesce (P_B_JOB, ''),
            coalesce (P_TAGS, '')
           );
    ;
  }
  return S;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.export_foaf (
  in domain_id integer,
  in entries any := null)
{
  declare S, T varchar;
  declare sStream any;
  declare st, msg, meta, rows any;

  sStream := string_output();
  S := 'sparql
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX sioc: <http://rdfs.org/sioc/ns#>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX atom: <http://atomowl.org/ontologies/atomrdf#>
        PREFIX vcard: <http://www.w3.org/2001/vcard-rdf/3.0#>
        PREFIX bio: <http://purl.org/vocab/bio/0.1/>
        CONSTRUCT {
	        ?person rdf:type ?rdfType .
	        ?person foaf:nick ?nick .
	        ?person foaf:name ?name .
	        ?person foaf:firstName ?firstName .
	        ?person foaf:family_name ?family_name .
	        ?person foaf:gender ?gender .
	        ?person foaf:mbox ?mbox .
	        ?person foaf:mbox_sha1sum ?mbox_sha1sum .
	        ?person foaf:icqChatID ?icqChatID .
	        ?person foaf:msnChatID ?msnChatID .
	        ?person foaf:aimChatID ?aimChatID .
	        ?person foaf:yahooChatID ?yahooChatID .
	        ?person foaf:birthday ?birthday .
	        ?person foaf:phone ?phone.
	        ?person foaf:based_near ?based_near .
	        ?based_near ?based_near_predicate ?based_near_subject .
	        ?person foaf:knows ?knows .
	        ?knows rdfs:seeAlso ?knows_seeAlso .
	        ?knows foaf:nick ?knows_nick .
	        ?person foaf:workplaceHomepage ?workplaceHomepage .
	        ?org foaf:homepage ?workplaceHomepage .
	        ?org rdf:type foaf:Organization .
	        ?org dc:title ?orgtit .
	        ?person foaf:homepage ?homepage .
	        ?person vcard:ADR ?adr .
	        ?adr vcard:Country ?country .
	        ?adr vcard:Region ?state .
          ?adr vcard:Locality ?city .
          ?adr vcard:Pcode ?pcode .
          ?adr vcard:Street ?street .
          ?adr vcard:Extadd ?extadd .
	        ?person bio:olb ?bio .
	        ?person bio:event ?event .
	        ?event rdf:type bio:Birth .
	        ?event dc:date ?bdate .
	      }
	      WHERE {
	        GRAPH <%s>
	        {
	          <FILTER>
	          ?person rdf:type ?rdfType .
            optional { ?person foaf:nick ?nick } .
            optional { ?person foaf:name ?name } .
            optional { ?person foaf:firstName ?firstName } .
            optional { ?person foaf:family_name ?family_name } .
            optional { ?person foaf:gender ?gender } .
            optional { ?person foaf:birthday ?birthday } .
            optional { ?person foaf:mbox ?mbox } .
            optional { ?person foaf:mbox_sha1sum ?mbox_sha1sum } .
            optional { ?person foaf:icqChatID ?icqChatID } .
            optional { ?person foaf:msnChatID ?msnChatID } .
            optional { ?person foaf:aimChatID ?aimChatID } .
            optional { ?person foaf:yahooChatID ?yahooChatID } .
            optional { ?person foaf:phone ?phone } .
            optional { ?person foaf:based_near ?based_near .
	                     ?based_near ?based_near_predicate ?based_near_subject .
	                   } .
            optional { ?person foaf:workplaceHomepage ?workplaceHomepage } .
            optional { ?org foaf:homepage ?workplaceHomepage .
	                     ?org a foaf:Organization ;
	                          dc:title ?orgtit .
	                   } .
            optional { ?person foaf:homepage ?homepage } .
            optional { ?person vcard:ADR ?adr .
	                     optional { ?adr vcard:Country ?country }.
		                   optional { ?adr vcard:Region ?state } .
		                   optional { ?adr vcard:Locality ?city } .
		                   optional { ?adr vcard:Pcode ?pcode  } .
		                   optional { ?adr vcard:Street ?street } .
		                   optional { ?adr vcard:Extadd ?extadd } .
		                 }
            optional { ?person bio:olb ?bio } .
            optional { ?person bio:event ?event.
                       ?event a bio:Birth ; dc:date ?bdate
                     }.
            optional { ?person foaf:knows ?knows .
	                     ?knows rdfs:seeAlso ?knows_seeAlso .
	                     ?knows foaf:nick ?knows_nick .
                     } .
	        }
	      }';

	S := sprintf (S, SIOC..get_graph ());
  T := '';
	if (not isnull (entries))
	{
	  foreach (any id in entries) do
	  {
	    if (T = '')
	    {
	      T := sprintf ('(?person = <%s>)', SIOC..socialnetwork_contact_iri (domain_id, cast (id as integer)));
	    } else {
	      T := T || ' || ' || sprintf ('(?person = <%s>)', SIOC..socialnetwork_contact_iri (domain_id, cast (id as integer)));
	    }
	  }
	  T := sprintf ('FILTER (%s) .', T);
	} else {
	  T := sprintf ('?person sioc:has_container <%s> .', SIOC..socialnetwork_iri (AB.WA.domain_name (domain_id)));
	}
  S := replace (S, '<FILTER>', T);
  st := '00000';
  exec (S, st, msg, vector (), 0, meta, rows);
  if ('00000' = st)
    DB.DBA.SPARQL_RESULTS_WRITE (sStream, meta, rows, '', 1);
  return string_output_string(sStream);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.uid ()
{
  return sprintf ('%s@%s', uuid (), sys_stat ('st_host_name'));
}
;

--------------------------------------------------------------------------------
--
create procedure AB.WA.exchange_exec (
  in _id integer,
  in _mode integer := 0,
  in _exMode integer := null)
{
  declare retValue any;

  declare exit handler for SQLSTATE '*'
  {
    update AB.WA.EXCHANGE
       set EX_EXEC_LOG = __SQL_STATE || ' ' || AB.WA.test_clear (__SQL_MESSAGE)
     where EX_ID = _id;
    commit work;

    if (_mode)
      resignal;
  };

  retValue := AB.WA.exchange_exec_internal (_id, _exMode);

  update AB.WA.EXCHANGE
     set EX_EXEC_TIME = now (),
         EX_EXEC_LOG = null,
         EX_UPDATE_SUBTYPE = null
   where EX_ID = _id;
  commit work;

  return retValue;
}
;

--------------------------------------------------------------------------------
--
create procedure AB.WA.exchange_entry_update (
  in _domain_id integer)
{
  for (select EX_ID as _id from AB.WA.EXCHANGE where EX_DOMAIN_ID = _domain_id and EX_TYPE = 0 and EX_UPDATE_TYPE = 1) do
  {
    if (connection_get ('__addressbook_import') = '1')
  {
      update AB.WA.EXCHANGE
         set EX_UPDATE_SUBTYPE = 1
       where EX_ID = _id;
    } else {
      AB.WA.exchange_exec (_id);
    }
  }
}
;

--------------------------------------------------------------------------------
--
create procedure AB.WA.exchange_exec_internal (
  in _id integer,
  in _exMode integer := null)
{
  for (select EX_DOMAIN_ID as _domain_id, EX_TYPE as _direction, deserialize (EX_OPTIONS) as _options from AB.WA.EXCHANGE where EX_ID = _id) do
  {
    declare _type, _name, _pName, _user, _password, _mode, _tags, _tagsInclude, _tagsExclude any;
    declare _content any;

    _type := get_keyword ('type', _options);
    _name := get_keyword ('name', _options);
    _user := get_keyword ('user', _options);
    _password := get_keyword ('password', _options);
    _mode := cast (get_keyword ('mode', _options) as integer);
    _tags := get_keyword ('tags', _options);
    _tagsInclude := get_keyword ('tagsInclude', _options);
    _tagsExclude := get_keyword ('tagsExclude', _options);

    -- publish
    if (_direction = 0)
    {
      _content := AB.WA.export_vcard (_domain_id, null, _options);
      if (_type = 1)
      {
        declare retValue, permissions any;
        {
          declare exit handler for SQLSTATE '*'
          {
            signal ('AB002', 'The export/publication did not pass successfully. Please verify the path and parameters values!<>');
          };
          permissions := AB.WA.dav_permissions (_name, _user, _password);
          _pName := replace (_name, ' ', '%20');
          _name := http_physical_path_resolve (_name);
          if (_name is null)
          {
            _name := _pName;
          }
          else if (_name not like '/DAV/%')
          {
            _name := '/DAV' || _name;
          }
          retValue := DB.DBA.DAV_RES_UPLOAD (_name, _content, 'text/text/x-vCard', permissions, _user, null, _user, _password);
          if (DB.DBA.DAV_HIDE_ERROR (retValue) is null)
          {
            signal ('AB001', 'WebDAV: ' || DB.DBA.DAV_PERROR (retValue) || '.<>');
          }
        }
      }
      else if (_type = 2)
      {
        declare retContent, resHeader, reqHeader any;

        reqHeader := null;
        if (_user <> '')
        {
          reqHeader := sprintf ('Authorization: Basic %s', encode_base64 (_user || ':' || _password));
        }
        commit work;
        {
          declare exit handler for SQLSTATE '*'
          {
            signal ('AB002', 'Connection Error in HTTP Client!<>');
          };
          retContent := http_get (_name, resHeader, 'PUT', reqHeader, _content);
          if (not (length (resHeader) > 0 and (resHeader[0] like 'HTTP/1._ 2__ %' or  resHeader[0] like 'HTTP/1._ 3__ %')))
          {
            signal ('AB002', 'The export/publication did not pass successfully. Please verify the path and parameters values!<>');
          }
        }
      }
    }
    -- subscribe
    else if (_direction = 1)
    {
      if (_type = 1)
      {
        _name := AB.WA.host_url () || _name;
      }
      _content := AB.WA.dav_content (_name, _user, _password);
      if (isnull(_content))
      {
        signal ('AB001', 'Bad import/subscription source!<>');
      }
      AB.WA.import_vcard (_domain_id, _content, _options);
    }
    -- syncml
    else if (_direction = 2)
    {
      declare data any;
      declare N, _in, _out, _rlog_res_id integer;
      declare _path, _pathID varchar;

      _in := 0;
      _out := 0;
      if (not isnull (_exMode))
      {
        _mode := _exMode;
      }
      if ((_mode >= 0) and AB.WA.dav_check_authenticate (_name, _user, _password, '1__'))
      {
      data := AB.WA.exec ('select distinct RLOG_RES_ID from DB.DBA.SYNC_RPLOG where RLOG_RES_COL = ? and DMLTYPE <> \'D\'', vector (DB.DBA.DAV_SEARCH_ID (_name, 'C')));
      for (N := 0; N < length (data); N := N + 1)
      {
        _rlog_res_id := data[N][0];
         for (select RES_CONTENT, RES_NAME, RES_MOD_TIME from WS.WS.SYS_DAV_RES where RES_ID = _rlog_res_id) do
        {
          connection_set ('__sync_dav_upl', '1');
            _in := _in + AB.WA.syncml2entry_internal (_domain_id, _name, _user, _password, _tags, RES_CONTENT, RES_NAME, RES_MOD_TIME, 1);
          connection_set ('__sync_dav_upl', '0');
        }
      }
      }

      if ((_mode <= 0) and AB.WA.dav_check_authenticate (_name, _user, _password, '11_'))
      {
      for (select P_ID, P_UID, P_TAGS from AB.WA.PERSONS where P_DOMAIN_ID = _domain_id) do
      {
          if (not AB.WA.tags_exchangeTest (P_TAGS, _tagsInclude, _tagsExclude))
          goto _skip;

        _path := _name || P_UID;
        _pathID := DB.DBA.DAV_SEARCH_ID (_path, 'R');
        if (not (isinteger(_pathID) and (_pathID > 0)))
        {
          AB.WA.syncml_entry_update_internal (_domain_id, P_ID, _path, _user, _password, 'I');
            _out := _out + 1;
        }

      _skip:;
      }
    }
      return vector (_in, _out);
    }
  }
}
;

--------------------------------------------------------------------------------
--
create procedure AB.WA.exchange_scheduler ()
{
  declare id, days, rc, err integer;
  declare bm any;

  declare _error integer;
  declare _bookmark any;
  declare _dt datetime;
  declare exID any;

  _dt := now ();
  declare cr static cursor for select EX_ID
                                 from AB.WA.EXCHANGE
                                where (EX_UPDATE_TYPE = 2 and (EX_EXEC_TIME is null or dateadd ('minute', EX_UPDATE_INTERVAL, EX_EXEC_TIME) < _dt))
                                   or (EX_UPDATE_TYPE = 1 and EX_UPDATE_SUBTYPE is not null);

  whenever not found goto _done;
  open cr (exclusive, prefetch 1);
  fetch cr into exID;
  while (1)
  {
    _bookmark := bookmark(cr);
    _error := 0;
    close cr;
    {
      declare exit handler for sqlstate '*'
      {
        _error := 1;

        goto _next;
      };
      AB.WA.exchange_exec (exID, 1);
      commit work;
    }

  _next:
    open cr (exclusive, prefetch 1);
    fetch cr bookmark _bookmark into exID;
    if (_error)
      fetch cr next into exID;
  }
_done:;
  close cr;
}
;


-----------------------------------------------------------------------------------------
--
create procedure AB.WA.dav_check_authenticate (
  in _path varchar,
  in _user varchar,
  in _password varchar,
  in _permissions varchar)
{
  declare _type varchar;

  _type := case when (strrchr (_path, '/') = length (_path) - 1) then 'C' else 'R' end;
  if (DB.DBA.DAV_AUTHENTICATE (DB.DBA.DAV_SEARCH_ID (_path, _type), _type, _permissions, _user, _password) < 0)
    return 0;
  return 1;
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.dav_parent (
  in path varchar)
{
  declare pos integer;

  path := trim (path, '/');
  pos := strrchr (path, '/');
  if (not isnull (pos))
    path := substring (path, 1, pos);
  return '/' || path || '/';
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.dav_permissions (
  in path varchar,
  in auth_name varchar := null,
  in auth_pwd varchar := null)
{
  declare uid, gid integer;
  declare permissions varchar;

  permissions := -1;
  permissions := DB.DBA.DAV_PROP_GET (path, ':virtpermissions', auth_name, auth_pwd);
  if (permissions < 0)
  {
    path := AB.WA.dav_parent (path);
    if (path <> AB.WA.dav_home (AB.WA.account_id (auth_name)))
      permissions := DB.DBA.DAV_PROP_GET (path, ':virtpermissions', auth_name, auth_pwd);
    if (permissions < 0)
      permissions := USER_GET_OPTION (auth_name, 'PERMISSIONS');
  }
  return permissions;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.syncml_check (
  in syncmlPath varchar := null)
{
  declare syncmlVersion varchar;

  syncmlVersion := DB.DBA.vad_check_version ('SyncML');
  if (not isstring (syncmlVersion))
    return 0;
  if (VAD.DBA.version_compare (syncmlVersion, '1.05.75') < 0)
    return 0;
  if (isnull (syncmlPath))
    return 1;
  if (DB.DBA.yac_syncml_version_get (syncmlPath) = 'N')
    return 0;
  if (DB.DBA.yac_syncml_type_get (syncmlPath) not in ('vcard_11', 'vcard_12'))
    return 0;
  return 1;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.syncml_entry_update (
  in _domain_id integer,
  in _entry_id integer,
  in _entry_gid varchar,
  in _entry_tags varchar,
  in _action varchar)
{
  declare _syncmlPath, _path, _user, _password, oTagsInclude, oTagsExclude any;

  if (connection_get ('__sync_dav_upl') = '1')
    return;

  for (select deserialize (EX_OPTIONS) as _options from AB.WA.EXCHANGE where EX_DOMAIN_ID = _domain_id and EX_TYPE = 2) do
  {
    _syncmlPath := get_keyword ('name', _options);

    if (not AB.WA.syncml_check (_syncmlPath))
      goto _skip;
    if (DB.DBA.yac_syncml_type_get (_syncmlPath) not in ('vcard_11', 'vcard_12'))
      goto _skip;

    oTagsInclude := null;
    oTagsExclude := null;
    if (not isnull (_options))
    {
      oTagsInclude := get_keyword ('tagsInclude', _options);
      oTagsExclude := get_keyword ('tagsExclude', _options);
    }
    if (not AB.WA.tags_exchangeTest (_entry_tags, oTagsInclude, oTagsExclude))
      goto _skip;

    _user := get_keyword ('user', _options);
    _password := get_keyword ('password', _options);
    _path := _syncmlPath || _entry_gid;

    AB.WA.syncml_entry_update_internal (_domain_id, _entry_id, _path, _user, _password, _action);

  _skip:;
  }
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.syncml_entry_update_internal (
  in _domain_id integer,
  in _entry_id integer,
  in _path varchar,
  in _user varchar,
  in _password varchar,
  in _action varchar)
{
  if ((_action = 'I') or (_action = 'U'))
  {
    declare _content, _permissions varchar;

    _content := AB.WA.entry2syncml (_entry_id);
    _permissions := USER_GET_OPTION (_user, 'PERMISSIONS');
    if (isnull (_permissions))
    {
      _permissions := '110100000RR';
    }
    connection_set ('__sync_dav_upl', '1');
    connection_set ('__sync_ods', '1');
    DB.DBA.DAV_RES_UPLOAD_STRSES_INT (_path, _content, 'text/x-vcard', _permissions, http_dav_uid (), http_dav_uid () + 1, null, null, 0);
    connection_set ('__sync_ods', '0');
    connection_set ('__sync_dav_upl', '0');
  }

  if (_action = 'D')
  {
    declare _id integer;

    _id := DB.DBA.DAV_SEARCH_ID (_path, 'R');
    if (isinteger(_id) and (_id > 0))
    {
      connection_set ('__sync_ods', '1');
      DB.DBA.DAV_DELETE (_path, 1, _user, _password);
      connection_set ('__sync_ods', '0');
    }
  }
}
;

----------------------------------------------------------------------
--
create procedure AB.WA.entry2syncml (
  in entry_id integer)
{
  declare S varchar;
  declare sStream any;

  sStream := string_output();
  for (select * from AB.WA.PERSONS where P_ID = entry_id) do
  {
    AB.WA.entry2syncml_line ('BEGIN', null, 'VCARD', sStream);
    AB.WA.entry2syncml_line ('VERSION', null, '2.1', sStream);
    AB.WA.entry2syncml_line ('REV', null, AB.WA.dt_iso8601 (P_UPDATED), sStream);

    -- personal
    AB.WA.entry2syncml_line ('UID', null, P_UID, sStream);
    AB.WA.entry2syncml_line ('NICKNAME', null, P_NAME, sStream);
    AB.WA.entry2syncml_line ('FN', null, P_FULL_NAME, sStream);

    -- Home
    S := coalesce (P_LAST_NAME, '');
    S := S || ';' || coalesce (P_FIRST_NAME, '');
    S := S || ';' || coalesce (P_MIDDLE_NAME, '');
    S := S || ';' || coalesce (P_TITLE, '');
    if (S <> ';;;')
    {
      AB.WA.entry2syncml_line ('N', null, S, sStream);
    }
    AB.WA.entry2syncml_line ('BDAY', null, AB.WA.dt_format (P_BIRTHDAY, 'Y-M-D'), sStream);

    -- mail
    AB.WA.entry2syncml_line ('EMAIL', vector ('TYPE', 'PREF', 'TYPE', 'INTERNET'), P_MAIL, sStream);
    -- web
    AB.WA.entry2syncml_line ('URL', null, P_WEB, sStream);
    AB.WA.entry2syncml_line ('URL', vector ('TYPE', 'HOME'), P_H_WEB, sStream);
    AB.WA.entry2syncml_line ('URL', vector ('TYPE', 'WORK'), P_B_WEB, sStream);
    -- Home
    S := ';';
    S := S || ''  || coalesce (P_H_ADDRESS1, '');
    S := S || ';' || coalesce (P_H_ADDRESS2, '');
    S := S || ';' || coalesce (P_H_CITY, '');
    S := S || ';' || coalesce (P_H_STATE, '');
    S := S || ';' || coalesce (P_H_CODE, '');
    S := S || ';' || coalesce (P_H_COUNTRY, '');
    if (S <> ';;;;;;')
    {
      AB.WA.entry2syncml_line ('ADR', vector ('TYPE', 'HOME'), S, sStream);
    }
    AB.WA.entry2syncml_line ('TS', null, P_H_TZONE, sStream);
    AB.WA.entry2syncml_line ('TEL', vector ('TYPE', 'HOME'), P_H_PHONE, sStream);
    AB.WA.entry2syncml_line ('TEL', vector ('TYPE', 'HOME', 'TYPE', 'CELL'), P_H_MOBILE, sStream);

    -- Business
    S := ';';
    S := S || ''  || coalesce (P_B_ADDRESS1, '');
    S := S || ';' || coalesce (P_B_ADDRESS2, '');
    S := S || ';' || coalesce (P_B_CITY, '');
    S := S || ';' || coalesce (P_B_STATE, '');
    S := S || ';' || coalesce (P_B_CODE, '');
    S := S || ';' || coalesce (P_B_COUNTRY, '');
    if (S <> ';;;;;;')
    {
      AB.WA.entry2syncml_line ('ADR', vector ('TYPE', 'WORK'), S, sStream);
    }
    AB.WA.entry2syncml_line ('TEL', vector ('TYPE', 'WORK'), P_B_PHONE, sStream);
    AB.WA.entry2syncml_line ('TEL', vector ('TYPE', 'WORK', 'TYPE', 'CELL'), P_B_MOBILE, sStream);

    AB.WA.entry2syncml_line ('ORG', null, P_B_ORGANIZATION, sStream);
    AB.WA.entry2syncml_line ('TITLE', null, P_B_JOB, sStream);

    AB.WA.entry2syncml_line ('END', null, 'VCARD', sStream);
  }

  return string_output_string (sStream);
}
;

----------------------------------------------------------------------
--
create procedure AB.WA.entry2syncml_line (
  in property varchar,
  in attrinutes any,
  in value any,
  inout sStream any)
{
  declare N imteger;
  declare S varchar;

  if (is_empty_or_null(value))
    return;

  S := '';
  if (not isnull(attrinutes))
  {
    for (N := 0; N < length(attrinutes); N := N + 2)
    {
      S := S || sprintf (' %s="%s"', attrinutes[N], attrinutes[N+1]);
    }
  }
  http (sprintf ('<%s%s><![CDATA[%s]]></%s>\r\n', property, S, cast (value as varchar), property), sStream);
}
;

----------------------------------------------------------------------
--
create procedure AB.WA.syncml2entry (
  in res_content varchar,
  in res_name varchar,
  in res_col varchar,
  in res_mod_time datetime := null)
{
  declare exit handler for sqlstate '*'
  {
    return;
  };

  declare _syncmlPath, _path, _user, _password, _tags varchar;

  for (select EX_DOMAIN_ID, deserialize (EX_OPTIONS) as _options from AB.WA.EXCHANGE where EX_TYPE = 2) do
  {
    _path := WS.WS.COL_PATH (res_col);
    _syncmlPath := get_keyword ('name', _options);
    if (_path = _syncmlPath)
    {
      _user := get_keyword ('user', _options);
      _password := get_keyword ('password', _options);
      _tags := get_keyword ('tags', _options);
      if (AB.WA.dav_check_authenticate (_path, _user, _password, '11_'))
        AB.WA.syncml2entry_internal (EX_DOMAIN_ID, _path, _user, _password, _tags, res_content, res_name, res_mod_time);
    }
  }
}
;

----------------------------------------------------------------------
--
create procedure AB.WA.syncml2entry_internal (
  in _domain_id integer,
  in _path varchar,
  in _user varchar,
  in _password varchar,
  in _tags varchar,
  in _res_content varchar,
  in _res_name varchar,
  in _res_mod_time datetime := null,
  in _internal integer := 0)
{
  declare exit handler for sqlstate '*'
  {
    return;
  };

  declare N, _pathID integer;
  declare _data, _uid  varchar;
  declare IDs any;

  if (not xslt_is_sheet ('http://local.virt/sync_out_xsl'))
    DB.DBA.sync_define_xsl ();

  _data := xtree_doc (_res_content, 0, '', 'utf-8');
  _data := xslt ('http://local.virt/sync_out_xsl', _data);
  _data := serialize_to_UTF8_xml (_data);
  _data := charset_recode (_data, 'UTF-8', '_WIDE_');

  if (not isinteger (_data))
  {
    IDs := AB.WA.import_vcard (_domain_id, _data, vector ('tags', _tags), null, _res_mod_time, _res_name);
    -- return;
    for (N := 0; N < length (IDs); N := N + 1)
    {
      _uid := (select P_UID from AB.WA.PERSONS where P_ID = IDs[N]);
      if (_uid <> _res_name)
      {
        _pathID := DB.DBA.DAV_SEARCH_ID (_path || _res_name, 'R');
        if (isinteger(_pathID) and (_pathID > 0))
        {
          if (_internal)
            set triggers off;

          update WS.WS.SYS_DAV_RES
             set RES_NAME = _uid,
                 RES_FULL_PATH = _path || _uid,
                 RES_CONTENT = AB.WA.entry2syncml (IDs[N])
           where RES_ID = _pathID;

          if (_internal)
            set triggers on;
        }
      }
    }
    return length (IDs);
  }
}
;

-------------------------------------------------------------------------------
--
create procedure AB.WA.search_sql (
  inout domain_id integer,
  inout account_id integer,
  inout data varchar,
  in maxRows varchar := '')
{
  declare S, T, tmp, where2, delimiter2 varchar;

  where2 := ' \n ';
  delimiter2 := '\n and ';

  S := '';
  if (not is_empty_or_null(AB.WA.xml_get('MyContacts', data)))
  {
    S := 'select                         \n' ||
         '  p.P_ID          P_ID,        \n' ||
         '  p.P_DOMAIN_ID   P_DOMAIN_ID, \n' ||
         '  p.P_NAME        P_NAME,      \n' ||
         '  p.P_TAGS        P_TAGS,      \n' ||
         '  p.P_CREATED     P_CREATED,   \n' ||
         '  p.P_UPDATED     P_UPDATED    \n' ||
         'from                           \n' ||
         '  AB.WA.PERSONS p              \n' ||
         'where p.P_DOMAIN_ID = <DOMAIN_ID> <TEXT> <WHERE>';
  }
  if (not is_empty_or_null(AB.WA.xml_get('MySharedContacts', data)))
  {
    if (S <> '')
      S := S || '\n union \n';
    S := S ||
         'select                         \n' ||
         '  p.P_ID          P_ID,        \n' ||
         '  p.P_DOMAIN_ID   P_DOMAIN_ID, \n' ||
         '  p.P_NAME        P_NAME,      \n' ||
         '  p.P_TAGS        P_TAGS,      \n' ||
         '  p.P_CREATED     P_CREATED,   \n' ||
         '  p.P_UPDATED     P_UPDATED    \n' ||
         'from                           \n' ||
         '  AB.WA.PERSONS p,             \n' ||
         '  AB.WA.GRANTS g               \n' ||
         'where p.P_ID = g.G_PERSON_ID   \n' ||
         '  and g.G_GRANTEE_ID = <ACCOUNT_ID> <TEXT> <WHERE>';
  }

  S := 'select <MAX> * from (' || S || ') x';

  T := '';
  tmp := AB.WA.xml_get('keywords', data);
  if (not is_empty_or_null(tmp))
  {
    T := FTI_MAKE_SEARCH_STRING(tmp);
  } else {
    tmp := AB.WA.xml_get('expression', data);
    if (not is_empty_or_null(tmp))
      T := tmp;
  }

  tmp := AB.WA.xml_get('tags', data);
  if (not is_empty_or_null(tmp))
  {
    if (T = '')
    {
      T := AB.WA.tags2search (tmp);
    } else {
      T := T || ' and ' || AB.WA.tags2search (tmp);
    }
  }
  if (T <> '')
    S := replace(S, '<TEXT>', sprintf('and contains(p.P_NAME, \'[__lang "x-ViDoc"] %s\') \n', T));

  tmp := AB.WA.xml_get('category', data);
  if (not is_empty_or_null(tmp))
  {
    where2 := ' and P_CATEGORY_ID = ' || tmp;
  }

  if (maxRows <> '')
    maxRows := 'TOP ' || maxRows;

  S := replace(S, '<MAX>', maxRows);
  S := replace(S, '<DOMAIN_ID>', cast(domain_id as varchar));
  S := replace(S, '<ACCOUNT_ID>', cast(account_id as varchar));
  S := replace(S, '<TEXT>', '');
  S := replace(S, '<WHERE>', where2);

  --dbg_obj_print(S);
  return S;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.category_update (
  in domain_id integer,
  in name varchar)
{
  declare id integer;

  name := trim (name);
  if (is_empty_or_null (name))
    return null;

  id := (select C_ID from AB.WA.CATEGORIES where C_DOMAIN_ID = domain_id and C_NAME = name);
  if (is_empty_or_null (id))
  {
    id := sequence_next ('AB.WA.category_id');
    insert into AB.WA.CATEGORIES (C_ID, C_DOMAIN_ID, C_NAME)
      values (id, domain_id, name);
  }
  return id;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.discussion_check ()
{
  if (isnull (VAD_CHECK_VERSION ('Discussion')))
    return 0;
  return 1;
}
;

-----------------------------------------------------------------------------------------
--
-- NNTP Conversation
--
-----------------------------------------------------------------------------------------
create procedure AB.WA.conversation_enable(
  in domain_id integer)
{
  return cast (get_keyword ('conv', AB.WA.settings(domain_id), '0') as integer);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.cm_root_node (
  in person_id varchar)
{
  declare root_id any;
  declare xt any;

  root_id := (select PC_ID from AB.WA.PERSON_COMMENTS where PC_PERSON_ID = person_id and PC_PARENT_ID is null);
  xt := (select xmlagg (xmlelement ('node', xmlattributes (PC_ID as id, PC_ID as name, PC_PERSON_ID as post)))
           from AB.WA.PERSON_COMMENTS
          where PC_PERSON_ID = person_id
            and PC_PARENT_ID = root_id
          order by PC_UPDATED);
  return xpath_eval ('//node', xt, 0);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.cm_child_node (
  in person_id varchar,
  inout node any)
{
  declare parent_id int;
  declare xt any;

  parent_id := xpath_eval ('number (@id)', node);
  person_id := xpath_eval ('@post', node);

  xt := (select xmlagg (xmlelement ('node', xmlattributes (PC_ID as id, PC_ID as name, PC_PERSON_ID as post)))
           from AB.WA.PERSON_COMMENTS
          where PC_PERSON_ID = person_id and PC_PARENT_ID = parent_id order by PC_UPDATED);
  return xpath_eval ('//node', xt, 0);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.make_rfc_id (
  in person_id integer,
  in comment_id integer := null)
{
  declare hashed, host any;

  hashed := md5 (uuid ());
  host := sys_stat ('st_host_name');
  if (isnull (comment_id))
    return sprintf ('<%d.%s@%s>', person_id, hashed, host);
  return sprintf ('<%d.%d.%s@%s>', person_id, comment_id, hashed, host);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.make_mail_subject (
  in txt any,
  in id varchar := null)
{
  declare enc any;

  enc := encode_base64 (AB.WA.wide2utf (txt));
  enc := replace (enc, '\r\n', '');
  txt := concat ('Subject: =?UTF-8?B?', enc, '?=\r\n');
  if (not isnull (id))
    txt := concat (txt, 'X-Virt-NewsID: ', uuid (), ';', id, '\r\n');
  return txt;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.make_post_rfc_header (
  in mid varchar,
  in refs varchar,
  in gid varchar,
  in title varchar,
  in rec datetime,
  in author_mail varchar)
{
  declare ses any;

  ses := string_output ();
  http (AB.WA.make_mail_subject (title), ses);
  http (sprintf ('Date: %s\r\n', DB.DBA.date_rfc1123 (rec)), ses);
  http (sprintf ('Message-Id: %s\r\n', mid), ses);
  if (not isnull (refs))
    http (sprintf ('References: %s\r\n', refs), ses);
  http (sprintf ('From: %s\r\n', author_mail), ses);
  http ('Content-Type: text/html; charset=UTF-8\r\n', ses);
  http (sprintf ('Newsgroups: %s\r\n\r\n', gid), ses);
  ses := string_output_string (ses);
  return ses;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.make_post_rfc_msg (
  inout head varchar,
  inout body varchar,
  in tree int := 0)
{
  declare ses any;

  ses := string_output ();
  http (head, ses);
  http (body, ses);
  http ('\r\n.\r\n', ses);
  ses := string_output_string (ses);
  if (tree)
    ses := serialize (mime_tree (ses));
  return ses;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.nntp_root (
  in domain_id integer,
  in person_id integer)
{
  declare owner_id integer;
  declare name, mail, title, comment any;

  owner_id := AB.WA.domain_owner_id (domain_id);
  name := AB.WA.account_fullName (owner_id);
  mail := AB.WA.account_mail (owner_id);

  select coalesce (P_NAME, ''), coalesce (P_FULL_NAME, '') into title, comment from AB.WA.PERSONS where P_ID = person_id;
  insert into AB.WA.PERSON_COMMENTS (PC_PARENT_ID, PC_DOMAIN_ID, PC_PERSON_ID, PC_TITLE, PC_COMMENT, PC_U_NAME, PC_U_MAIL, PC_CREATED, PC_UPDATED)
    values (null, domain_id, person_id, title, comment, name, mail, now (), now ());
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.nntp_update_item (
  in domain_id integer,
  in person_id integer)
{
  declare grp, ngnext integer;
  declare nntpName, rfc_id varchar;

  nntpName := AB.WA.domain_nntp_name (domain_id);
  select NG_GROUP, NG_NEXT_NUM into grp, ngnext from DB..NEWS_GROUPS where NG_NAME = nntpName;
  select PC_RFC_ID into rfc_id from AB.WA.PERSON_COMMENTS where PC_DOMAIN_ID = domain_id and PC_PERSON_ID = person_id and PC_PARENT_ID is null;
  if (exists (select 1 from DB.DBA.NEWS_MULTI_MSG where NM_KEY_ID = rfc_id and NM_GROUP = grp))
    return;

  if (ngnext < 1)
    ngnext := 1;

  for (select PC_RFC_ID as rfc_id from AB.WA.PERSON_COMMENTS where PC_DOMAIN_ID = domain_id and PC_PERSON_ID = person_id) do
  {
    insert soft DB.DBA.NEWS_MULTI_MSG (NM_KEY_ID, NM_GROUP, NM_NUM_GROUP) values (rfc_id, grp, ngnext);
    ngnext := ngnext + 1;
  }

  set triggers off;
  update DB.DBA.NEWS_GROUPS set NG_NEXT_NUM = ngnext + 1 where NG_NAME = nntpName;
  DB.DBA.ns_up_num (grp);
  set triggers on;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.nntp_update (
  in domain_id integer,
  in oInstance varchar,
  in nInstance varchar,
  in oConversation integer := null,
  in nConversation integer := null)
{
  declare nntpGroup integer;
  declare nDescription varchar;

  if (isnull (oInstance))
    oInstance := AB.WA.domain_nntp_name (domain_id);

  if (isnull (nInstance))
    nInstance := AB.WA.domain_nntp_name (domain_id);

  nDescription := AB.WA.domain_description (domain_id);

  if (isnull (nConversation))
  {
    update DB.DBA.NEWS_GROUPS
      set NG_POST = 1,
          NG_NAME = nInstance,
          NG_DESC = nDescription
    where NG_NAME = oInstance;
    return;
  }

  if (oConversation = 1 and nConversation = 0)
  {
    nntpGroup := (select NG_GROUP from DB.DBA.NEWS_GROUPS where NG_NAME = oInstance);
    delete from DB.DBA.NEWS_MULTI_MSG where NM_GROUP = nntpGroup;
    delete from DB.DBA.NEWS_GROUPS where NG_NAME = oInstance;
  }
  else if (oConversation = 0 and nConversation = 1)
  {
    declare exit handler for sqlstate '*' { return; };

    insert into DB.DBA.NEWS_GROUPS (NG_NEXT_NUM, NG_NAME, NG_DESC, NG_SERVER, NG_POST, NG_UP_TIME, NG_CREAT, NG_UP_INT, NG_PASS, NG_UP_MESS, NG_NUM, NG_FIRST, NG_LAST, NG_LAST_OUT, NG_CLEAR_INT, NG_TYPE)
      values (0, nInstance, nDescription, null, 1, now(), now(), 30, 0, 0, 0, 0, 0, 0, 120, 'ADDRESSBOOK');
  }
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.nntp_fill (
  in domain_id integer)
{
  declare exit handler for SQLSTATE '*', not found {
    return;
  };

  declare grp, ngnext integer;
  declare nntpName varchar;

  for (select P_ID from AB.WA.PERSONS where P_DOMAIN_ID = domain_id) do
  {
    if (not exists (select 1 from AB.WA.PERSON_COMMENTS where PC_DOMAIN_ID = domain_id and PC_PERSON_ID = P_ID and PC_PARENT_ID is null))
      AB.WA.nntp_root (domain_id, P_ID);
  }
  nntpName := AB.WA.domain_nntp_name (domain_id);
  select NG_GROUP, NG_NEXT_NUM into grp, ngnext from DB..NEWS_GROUPS where NG_NAME = nntpName;
  if (ngnext < 1)
    ngnext := 1;

  for (select PC_RFC_ID as rfc_id from AB.WA.PERSON_COMMENTS where PC_DOMAIN_ID = domain_id) do
  {
    insert soft DB.DBA.NEWS_MULTI_MSG (NM_KEY_ID, NM_GROUP, NM_NUM_GROUP) values (rfc_id, grp, ngnext);
    ngnext := ngnext + 1;
  }

  set triggers off;
  update DB.DBA.NEWS_GROUPS set NG_NEXT_NUM = ngnext + 1 where NG_NAME = nntpName;
  DB.DBA.ns_up_num (grp);
  set triggers on;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.mail_address_split (
  in author any,
  out person any,
  out email any)
{
  declare pos int;

  person := '';
  pos := strchr (author, '<');
  if (pos is not NULL)
  {
    person := "LEFT" (author, pos);
    email := subseq (author, pos, length (author));
    email := replace (email, '<', '');
    email := replace (email, '>', '');
    person := trim (replace (person, '"', ''));
  } else {
    pos := strchr (author, '(');
    if (pos is not NULL)
    {
      email := trim ("LEFT" (author, pos));
      person :=  subseq (author, pos, length (author));
      person := replace (person, '(', '');
      person := replace (person, ')', '');
    }
  }
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.nntp_decode_subject (
  inout str varchar)
{
  declare match varchar;
  declare inx int;

  inx := 50;

  str := replace (str, '\t', '');

  match := regexp_match ('=\\?[^\\?]+\\?[A-Z]\\?[^\\?]+\\?=', str);
  while (match is not null and inx > 0)
  {
    declare enc, ty, dat, tmp, cp, dec any;

    cp := match;
    tmp := regexp_match ('^=\\?[^\\?]+\\?[A-Z]\\?', match);

    match := substring (match, length (tmp)+1, length (match) - length (tmp) - 2);

    enc := regexp_match ('=\\?[^\\?]+\\?', tmp);

    tmp := replace (tmp, enc, '');

    enc := trim (enc, '?=');
    ty := trim (tmp, '?');

    if (ty = 'B')
    {
      dec := decode_base64 (match);
    } else if (ty = 'Q') {
      dec := uudecode (match, 12);
    } else {
      dec := '';
    }
    declare exit handler for sqlstate '2C000' { return;};
    dec := charset_recode (dec, enc, 'UTF-8');

    str := replace (str, cp, dec);

    match := regexp_match ('=\\?[^\\?]+\\?[A-Z]\\?[^\\?]+\\?=', str);
    inx := inx - 1;
  }
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.nntp_process_parts (
  in parts any,
  inout body varchar,
  inout amime any,
  out result any,
  in any_part int)
{
  declare name1, mime1, name, mime, enc, content, charset varchar;
  declare i, i1, l1, is_allowed int;
  declare part any;

  if (not isarray (result))
    result := vector ();

  if (not isarray (parts) or not isarray (parts[0]))
    return 0;

  part := parts[0];

  name1 := get_keyword_ucase ('filename', part, '');
  if (name1 = '')
    name1 := get_keyword_ucase ('name', part, '');

  mime1 := get_keyword_ucase ('Content-Type', part, '');
  charset := get_keyword_ucase ('charset', part, '');

  if (mime1 = 'application/octet-stream' and name1 <> '')
    mime1 := http_mime_type (name1);

  is_allowed := 0;
  i1 := 0;
  l1 := length (amime);
  while (i1 < l1)
  {
    declare elm any;
    elm := trim(amime[i1]);
    if (mime1 like elm)
    {
      is_allowed := 1;
      i1 := l1;
    }
    i1 := i1 + 1;
  }

  declare _cnt_disp any;
  _cnt_disp := get_keyword_ucase('Content-Disposition', part, '');

  if (is_allowed and (any_part or (name1 <> '' and _cnt_disp in ('attachment', 'inline'))))
  {
    name := name1;
    mime := mime1;
    enc := get_keyword_ucase ('Content-Transfer-Encoding', part, '');
    content := subseq (body, parts[1][0], parts[1][1]);
    if (enc = 'base64')
      content := decode_base64 (content);
    result := vector_concat (result, vector (vector (name, mime, content, _cnt_disp, enc, charset)));
    return 1;
  }

  -- process the parts
  if (isarray (parts[2]))
    for (i := 0; i < length (parts[2]); i := i + 1)
      AB.WA.nntp_process_parts (parts[2][i], body, amime, result, any_part);

  return 0;
}
;

-----------------------------------------------------------------------------------------
--
DB.DBA.NNTP_NEWS_MSG_ADD (
'ADDRESSBOOK',
'select
   \'ADDRESSBOOK\',
   PC_RFC_ID,
   PC_RFC_REFERENCES,
   0,    -- NM_READ
   null,
   PC_UPDATED,
   0,    -- NM_STAT
   null, -- NM_TRY_POST
   0,    -- NM_DELETED
   AB.WA.make_post_rfc_msg (PC_RFC_HEADER, PC_COMMENT, 1), -- NM_HEAD
   AB.WA.make_post_rfc_msg (PC_RFC_HEADER, PC_COMMENT),
   PC_ID
 from AB.WA.PERSON_COMMENTS'
)
;

-----------------------------------------------------------------------------------------
--
create procedure DB.DBA.ADDRESSBOOK_NEWS_MSG_I (
  inout N_NM_ID any,
  inout N_NM_REF any,
  inout N_NM_READ any,
  inout N_NM_OWN any,
  inout N_NM_REC_DATE any,
  inout N_NM_STAT any,
  inout N_NM_TRY_POST any,
  inout N_NM_DELETED any,
  inout N_NM_HEAD any,
  inout N_NM_BODY any)
{
  declare uid, parent_id, domain_id, item_id any;
  declare author, name, mail, tree, head, contentType, content, subject, title, cset any;
  declare rfc_id, rfc_header, rfc_references, refs any;

  uid := connection_get ('nntp_uid');
  if (isnull (N_NM_REF) and isnull (uid))
    signal ('CONVA', 'The post cannot be done via news client, this requires authentication.');

  tree := deserialize (N_NM_HEAD);
  head := tree [0];
  contentType := get_keyword_ucase ('Content-Type', head, 'text/plain');
  cset  := upper (get_keyword_ucase ('charset', head));
  author :=  get_keyword_ucase ('From', head, 'nobody@unknown');
  subject :=  get_keyword_ucase ('Subject', head);

  if (not isnull (subject))
    AB.WA.nntp_decode_subject (subject);

  if (contentType like 'text/%')
  {
    declare st, en int;
    declare last any;

    st := tree[1][0];
    en := tree[1][1];

    if (en > st + 5) {
      last := subseq (N_NM_BODY, en - 4, en);
      if (last = '\r\n.\r')
        en := en - 4;
    }
    content := subseq (N_NM_BODY, st, en);
    if (cset is not null and cset <> 'UTF-8')
    {
      declare exit handler for sqlstate '2C000' { goto next_1;};
      content := charset_recode (content, cset, 'UTF-8');
    }
  next_1:;
    if (contentType = 'text/plain')
      content := '<pre>' || content || '</pre>';
  }
  else if (contentType like 'multipart/%')
  {
    declare res, best_cnt any;

    declare exit handler for sqlstate '*' {  signal ('CONVX', __SQL_MESSAGE);};

    AB.WA.nntp_process_parts (tree, N_NM_BODY, vector ('text/%'), res, 1);

    best_cnt := null;
    content := null;
    foreach (any elm in res) do
    {
      if (elm[1] = 'text/html' and (content is null or best_cnt = 'text/plain'))
      {
        best_cnt := 'text/html';
        content := elm[2];
        if (elm[4] = 'quoted-printable')
        {
          content := uudecode (content, 12);
        } else if (elm[4] = 'base64') {
          content := decode_base64 (content);
        }
        cset := elm[5];
      } else if (best_cnt is null and elm[1] = 'text/plain') {
        content := elm[2];
        best_cnt := 'text/plain';
        cset := elm[5];
      }
      if (elm[1] not like 'text/%')
        signal ('CONVX', sprintf ('The post contains parts of type [%s] which is prohibited.', elm[1]));
    }
    if (length (cset) and cset <> 'UTF-8')
    {
      declare exit handler for sqlstate '2C000' { goto next_2;};
      content := charset_recode (content, cset, 'UTF-8');
    }
  next_2:;
  } else
    signal ('CONVX', sprintf ('The content type [%s] is not supported', contentType));

  rfc_header := '';
  for (declare i int, i := 0; i < length (head); i := i + 2)
  {
    if (lower (head[i]) <> 'content-type' and lower (head[i]) <> 'mime-version' and lower (head[i]) <> 'boundary'  and lower (head[i]) <> 'subject')
      rfc_header := rfc_header || head[i] ||': ' || head[i + 1]||'\r\n';
  }
  rfc_header := AB.WA.make_mail_subject (subject) || rfc_header || 'Content-Type: text/html; charset=UTF-8\r\n\r\n';

  rfc_references := N_NM_REF;

  if (not isnull (N_NM_REF))
  {
    declare exit handler for not found { signal ('CONV1', 'No such article.');};

    parent_id := null;
    refs := split_and_decode (N_NM_REF, 0, '\0\0 ');
    if (length (refs))
      N_NM_REF := refs[length (refs) - 1];

    select PC_ID, PC_DOMAIN_ID, PC_PERSON_ID, PC_TITLE
      into parent_id, domain_id, item_id, title
      from AB.WA.PERSON_COMMENTS
     where PC_RFC_ID = N_NM_REF;

    if (isnull (subject))
      subject := 'Re: '|| title;

    AB.WA.mail_address_split (author, name, mail);

    insert into AB.WA.PERSON_COMMENTS (PC_PARENT_ID, PC_DOMAIN_ID, PC_PERSON_ID, PC_TITLE, PC_COMMENT, PC_U_NAME, PC_U_MAIL, PC_UPDATED, PC_RFC_ID, PC_RFC_HEADER, PC_RFC_REFERENCES)
      values (parent_id, domain_id, item_id, subject, content, name, mail, N_NM_REC_DATE, N_NM_ID, rfc_header, rfc_references);
  }
}
;

-----------------------------------------------------------------------------------------
--
create procedure DB.DBA.ADDRESSBOOK_NEWS_MSG_U (
  inout O_NM_ID any,
  inout N_NM_ID any,
  inout N_NM_REF any,
  inout N_NM_READ any,
  inout N_NM_OWN any,
  inout N_NM_REC_DATE any,
  inout N_NM_STAT any,
  inout N_NM_TRY_POST any,
  inout N_NM_DELETED any,
  inout N_NM_HEAD any,
  inout N_NM_BODY any)
{
  return;
}
;

-----------------------------------------------------------------------------------------
--
create procedure DB.DBA.ADDRESSBOOK_NEWS_MSG_D (
  inout O_NM_ID any)
{
  signal ('CONV3', 'Delete of a Person comment is not allowed');
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.news_comment_get_mess_attachments (inout _data any, in get_uuparts integer)
{
  declare data, outp, _all any;
  declare line varchar;
  declare in_UU, get_body integer;

  data := string_output (http_strses_memory_size ());
  http (_data, data);
  http ('\n', data);
  _all := vector ();

  outp := string_output (http_strses_memory_size ());

  in_UU := 0;
  get_body := 1;
  while (1 = 1)
  {
    line := ses_read_line (data, 0);

    if (line is null or isstring (line) = 0)
    {
      if (length (_all) = 0)
      {
        _all := vector_concat (_all, vector (string_output_string (outp)));
      }
      return _all;
    }
    if (in_UU = 0 and subseq (line, 0, 6) = 'begin ' and length (line) > 6)
    {
      in_UU := 1;
      if (get_body)
      {
        get_body := 0;
        _all := vector_concat (_all, vector (string_output_string (outp)));
        http_output_flush (outp);
      }
      _all := vector_concat (_all, vector (subseq (line, 10)));
    }
    else if (in_UU = 1 and subseq (line, 0, 3) = 'end')
    {
      in_UU := 0;
      if (get_uuparts)
      {
        _all := vector_concat (_all, vector (string_output_string (outp)));
        http_output_flush (outp);
      }
    }
    else if ((get_uuparts and in_UU = 1) or get_body)
    {
      http (line, outp);
      http ('\n', outp);
    }
  }
  return _all;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.news_comment_get_cn_type (in f_name varchar)
{
  declare ext varchar;
  declare temp any;

  ext := 'text/html';
  temp := split_and_decode (f_name, 0, '\0\0.');

  if (length (temp) < 2)
    return ext;

  temp := temp[1];

  if (exists (select 1 from WS.WS.SYS_DAV_RES_TYPES where T_EXT = temp))
	  ext := ((select T_TYPE from WS.WS.SYS_DAV_RES_TYPES where T_EXT = temp));

  return ext;
}
;


-----------------------------------------------------------------------------------------
--
-- API Lib Procedures
--
-----------------------------------------------------------------------------------------
create procedure AB.WA.owner2contactMap ()
{
  return vector (
    'U_ID'           , 'P_ID'            ,
    'U_NAME'         , 'P_NAME'          ,
    'U_E_MAIL'       , 'P_MAIL'          ,
    'WAUI_TITLE'     , 'P_TITLE'         ,
    'WAUI_BIRTHDAY'  , 'P_BIRTHDAY'      ,
    'WAUI_GENDER'    , 'P_GENDER'        ,
    'WAUI_FIRST_NAME', 'P_FIRST_NAME'    ,
    'WAUI_LAST_NAME' , 'P_LAST_NAME'     ,
    'WAUI_GENDER'    , 'P_GENDER'        ,
    'WAUI_ICQ'       , 'P_ICQ'           ,
    'WAUI_SKYPE'     , 'P_SKYPE'         ,
    'WAUI_YAHOO'     , 'P_YAHOO'         ,
    'WAUI_AIM'       , 'P_AIM'           ,
    'WAUI_MSN'       , 'P_MSN'           ,
    'WAUI_HCOUNTRY'  , 'P_H_COUNTRY'     ,
    'WAUI_HSTATE'    , 'P_H_STATE'       ,
    'WAUI_HCITY'     , 'P_H_CITY'        ,
    'WAUI_HCODE'     , 'P_H_CODE'        ,
    'WAUI_HADDRESS1' , 'P_H_ADDRESS1'    ,
    'WAUI_HADDRESS2' , 'P_H_ADDRESS2'    ,
    'WAUI_HTZONE'    , 'P_H_TZONE'       ,
    'WAUI_LAT'       , 'P_H_LAT'         ,
    'WAUI_LNG'       , 'P_H_LNG'         ,
    'WAUI_HPHONE'    , 'P_H_PHONE'       ,
    'WAUI_HMOBILE'   , 'P_H_MOBILE'      ,
    'WAUI_BCOUNTRY'  , 'P_B_COUNTRY'     ,
    'WAUI_BSTATE'    , 'P_B_STATE'       ,
    'WAUI_BCITY'     , 'P_B_CITY'        ,
    'WAUI_BCODE'     , 'P_B_CODE'        ,
    'WAUI_BADDRESS1' , 'P_B_ADDRESS1'    ,
    'WAUI_BADDRESS2' , 'P_B_ADDRESS2'    ,
    'WAUI_BTZONE'    , 'P_B_TZONE'       ,
    'WAUI_BLAT'      , 'P_B_LAT'         ,
    'WAUI_BLNG'      , 'P_B_LNG'         ,
    'WAUI_BPHONE'    , 'P_B_PHONE'       ,
    'WAUI_BMOBILE'   , 'P_B_MOBILE'      ,
    'WAUI_WEBPAGE'   , 'P_WEB'           ,
    'WAUI_BINDUSTRY' , 'P_B_INDUSTRY'    ,
    'WAUI_BORG'      , 'P_B_ORGANIZATION',
    'WAUI_BJOB'      , 'P_B_JOB'         ,
    'WAUI_TAGS'      , 'P_TAGS'          );
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.apiHTTPError (
  in error varchar)
{
  declare S varchar;

  S := replace (error, 'HTTP/1.1 ', '');
  http_header ('Content-Type: text/html; charset=UTF-8\r\n');
  http_request_status (error);
  http (sprintf ('<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN"><html><head><title>%s</title></head><body><h1>%s</h1></body></html>', S, S));
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.apiObject ()
{
  return subseq (soap_box_structure ('x', 1), 0, 2);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.isApiObject (inout o any)
{
  if (isarray (o) and (length (o) > 1) and (__tag (o[0]) = 255))
    return 1;
  return 0;
}
;


-----------------------------------------------------------------------------------------
--
create procedure AB.WA.obj2xml (
  in o any,
  in d integer := 3,
  in tag varchar := null)
{
  declare N, M integer;
  declare R, T any;
  declare retValue any;

  if (d = 0)
    return '[maximum depth achieved]';

  retValue := '';
  if (isnumeric (o))
  {
    retValue := cast (o as varchar);
  }
  else if (isstring (o))
  {
    retValue := sprintf ('%V', o);
  }
  else if (AB.WA.isApiObject (o))
  {
    for (N := 2; N < length(o); N := N + 2)
    {
      if (not AB.WA.isApiObject (o[N+1]) and isarray (o[N+1]) and not isstring (o[N+1]))
      {
        retValue := retValue || AB.WA.obj2xml (o[N+1], d-1, o[N]);
      } else {
        retValue := retValue || sprintf ('<%s>%s</%s>\n', o[N],  AB.WA.obj2xml (o[N+1], d-1), o[N]);
      }
    }
  }
  else if (isarray (o))
  {
    for (N := 0; N < length(o); N := N + 1)
    {
      if (isnull (tag))
      {
        retValue := retValue || AB.WA.obj2xml (o[N], d-1);
      } else {
        retValue := retValue || sprintf ('<%s>%s</%s>\n', tag,  AB.WA.obj2xml (o[N], d-1), tag);
      }
    }
  }
  return retValue;
}
;

-----------------------------------------------------------------------------------------
--
-- Portable Contacts
--
-----------------------------------------------------------------------------------------
create procedure AB.WA.pcMap ()
{
  return vector ('id',              vector ('field',    'P_ID'),
                 'nickname',        vector ('field',    'P_NAME'),
                 'gender',          vector ('field',    'P_GENDER'),
                 'name.givenName',  vector ('field',    'P_FIRST_NAME'),
                 'name.middleName', vector ('field',    'P_MIDDLE_NAME'),
                 'name.familyName', vector ('field',    'P_LAST_NAME'),
                 'published',       vector ('type',     'function',
                                            'field',    vector ('P_CREATED',   vector ('function', 'select AB.WA.dt_iso8601 (?)'))
                                           ),
                 'updated',         vector ('type',     'function',
                                            'field',    vector ('P_UPDATED',   vector ('function', 'select AB.WA.dt_iso8601 (?)'))
                                           ),
                 'birthday',        vector ('type',     'function',
                                            'field',    vector ('P_BIRTHDAY',  vector ('function', 'select subseq (AB.WA.dt_iso8601 (?), 0, 10)'))
                                           ),
                 'emails',          vector ('sort',     'P_MAIL',
                                            'filter',   vector ('P_MAIL', 'P_H_MAIL', 'P_B_MAIL'),
                                            'type',     'vector/object',
                                            'field',    vector ('P_MAIL',   vector ('template', vector_concat (AB.WA.apiObject (), vector ('primary', 'true'))),
                                                                'P_H_MAIL', vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_B_MAIL', vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'work')))
                                                               )
                                           ),
                 'phoneNumbers',    vector ('sort',     'P_PHONE',
                                            'filter',   vector ('P_PHONE', 'P_H_PHONE', 'P_B_PHONE'),
                                            'type',     'vector/object',
                                            'field',    vector ('P_PHONE',   vector ('template', vector_concat (AB.WA.apiObject (), vector ('primary', 'true'))),
                                                                'P_H_PHONE', vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_B_PHONE', vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'work')))
                                                               )
                                           ),
                 'urls',            vector ('sort',     'P_H_WEB',
                                            'filter',   vector ('P_WEB', 'P_H_WEB', 'P_B_WEB'),
                                            'type',     'vector/object',
                                            'field',    vector ('P_WEB',     vector ('template', vector_concat (AB.WA.apiObject (), vector ('primary', 'true'))),
                                                                'P_H_WEB',   vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_B_WEB',   vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'work')))
                                                               )
                                           ),
                 'ims',             vector ('sort',     'null',
                                            'filter',   'null',
                                            'type',     'vector/object',
                                            'field',    vector ('P_ICQ',   vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'icq'))),
                                                                'P_SKYPE', vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'skype'))),
                                                                'P_AIM',   vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'aim'))),
                                                                'P_YAHOO', vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'yahoo'))),
                                                                'P_MSN',   vector ('template', vector_concat (AB.WA.apiObject (), vector ('type', 'msn')))
                                                               )
                                           ),
                 'addresses',       vector ('sort',     'null',
                                            'filter',   'null',
                                            'type',     'vector/object2',
                                            'unique',   'type',
                                            'field',    vector ('P_H_COUNTRY', vector ('property', 'country',       'template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_H_STATE',   vector ('property', 'region',        'template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_H_CODE',    vector ('property', 'postalCode',    'template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_H_CITY',    vector ('property', 'locality',      'template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_H_ADDRESS1',vector ('property', 'streetAddress', 'template', vector_concat (AB.WA.apiObject (), vector ('type', 'home'))),
                                                                'P_B_COUNTRY', vector ('property', 'country',       'template', vector_concat (AB.WA.apiObject (), vector ('type', 'work'))),
                                                                'P_B_STATE',   vector ('property', 'region',        'template', vector_concat (AB.WA.apiObject (), vector ('type', 'work'))),
                                                                'P_B_CODE',    vector ('property', 'postalCode',    'template', vector_concat (AB.WA.apiObject (), vector ('type', 'work'))),
                                                                'P_B_CITY',    vector ('property', 'locality',      'template', vector_concat (AB.WA.apiObject (), vector ('type', 'work'))),
                                                                'P_B_ADDRESS1',vector ('property', 'streetAddress', 'template', vector_concat (AB.WA.apiObject (), vector ('type', 'work')))
                                                               )
                                           ),
                 'tags',            vector ('sort',     'null',
                                            'filter',   'null',
                                            'type',     'function',
                                            'field',    vector ('P_TAGS',   vector ('function', 'select AB.WA.tags2vector (?)'))
                                           )
                );
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.pcSortField (
  in pcField varchar,
  in pcMap varchar)
{
  declare V, F any;

  V := get_keyword (pcField, pcMap);
  if (isnull (V))
    return V;

  F := get_keyword ('sort', V);
  if (isstring (F) and (F = 'null'))
    return null;
  if (isstring (F))
    return F;
  F := get_keyword ('field', V);
  if (isstring (F))
    return F;
  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.pcFilterField (
  in pcField varchar,
  in pcMap varchar)
{
  declare V, F any;

  V := get_keyword (pcField, pcMap);
  if (isnull (V))
    return V;

  F := get_keyword ('filter', V);
  if (isstring (F) and (F = 'null'))
    return null;
  if (not isarray (F))
    F := get_keyword ('field', V);
  if (isstring (F))
    F := vector (F);
  if (isarray (F))
    return F;
  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.pcContactField (
  in pcField varchar,
  in pcMap varchar,
  in pcFields any := null)
{
  if (not isnull (pcFields) and not AB.WA.vector_contains (pcFields, pcField))
    return null;

  return get_keyword (pcField, pcMap);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.simplifyMeta (
  in abMeta any)
{
  declare N integer;
  declare newMeta any;

  newMeta := vector ();
  for (N := 0; N < length (abMeta[0]); N := N + 1)
    newMeta := vector_concat (newMeta, vector (abMeta[0][N][0]));

  return newMeta;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.pcContactFieldIndex (
  in abField varchar,
  in abMeta any)
{
  declare N integer;

  for (N := 0; N < length (abMeta); N := N + 1)
    if (abField = abMeta[N])
      return N;

  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.pcContactObject (
  in fields any,
  in data any,
  in meta any,
  in pcMap any)
{
  declare K, L, M, N integer;
  declare oEntry any;
  declare R, V, T, P, P1, aIndex any;
  declare tmp, pcField, pcFieldDef, pcUnique, pcUniqueValue, abValue, abFieldDef, abField, abFieldType, abFieldIndex any;

  oEntry := AB.WA.apiObject ();
  for (N := 0; N < length (fields); N := N + 1)
  {
    pcField := fields[N];
    pcFieldDef := AB.WA.pcContactField (pcField, pcMap);
    if (isnull (pcFieldDef))
      goto _skip;
    abField := get_keyword ('field', pcFieldDef);
    if (isnull (abField))
      goto _skip;
    abFieldType := get_keyword ('type', pcFieldDef);
    if (isstring (abField))
      abField := vector (abField, null);
    for (M := 0; M < length (abField); M := M + 2)
    {
      abFieldDef := abField[M+1];
      abFieldIndex := AB.WA.pcContactFieldIndex (abField[M], meta);
      if (isnull (abFieldIndex))
        goto _skip_field;
      abValue := data[abFieldIndex];
      if (is_empty_or_null (abValue))
        goto _skip_field;
      if (isnull (abFieldType))
      {
        if (isnull (strchr (pcField, '.')))
        {
          oEntry := vector_concat (oEntry, vector (pcField, abValue));
        }
        else
        {
          V := split_and_decode (pcField, 0, '\0\0.');
          if (length (V) <> 2)
            goto _skip_field;
          if (isnull (AB.WA.vector_index (oEntry, V[0])))
            oEntry := vector_concat (oEntry, vector (V[0], AB.WA.apiObject ()));
          T := get_keyword (V[0], oEntry);
          T := AB.WA.set_keyword (V[1], T, abValue);
          oEntry := AB.WA.set_keyword (V[0], oEntry, T);
        }
      }
      else if (abFieldType = 'function')
      {
        tmp := get_keyword ('function', abFieldDef);
        if (not isnull (tmp))
        {
          tmp := AB.WA.exec (tmp, vector (abValue));
          if (length (tmp))
            abValue := tmp[0][0];
        }
        oEntry := vector_concat (oEntry, vector (pcField, abValue));
      }
      else if (abFieldType = 'vector/object')
      {
        if (isnull (AB.WA.vector_index (oEntry, pcField)))
          oEntry := vector_concat (oEntry, vector (pcField, vector ()));
        P := get_keyword (pcField, oEntry);
        T := get_keyword ('template', abFieldDef, AB.WA.apiObject ());
        T := AB.WA.set_keyword (get_keyword ('property', abFieldDef, 'value'), T, abValue);
        P := vector_concat (P, vector (T));
        oEntry := AB.WA.set_keyword (pcField, oEntry, P);
      }
      else if (abFieldType = 'vector/object2')
      {
        if (isnull (AB.WA.vector_index (oEntry, pcField)))
          oEntry := vector_concat (oEntry, vector (pcField, vector ()));
        P := get_keyword (pcField, oEntry);
        T := get_keyword ('template', abFieldDef, AB.WA.apiObject ());
        pcUnique := get_keyword ('unique', pcFieldDef, 'type');
        pcUniqueValue := get_keyword (pcUnique, T);
        K := -1;
        for (L := 0; L < length (P); L := L + 1)
        {
          if (get_keyword (pcUnique, P[L]) = pcUniqueValue)
          {
            K := L;
            goto _exit_unique;
          }
        }
        P := vector_concat (P, vector (T));
        K := length (P) - 1;
      _exit_unique:;
        T := P[K];
        T := AB.WA.set_keyword (get_keyword ('property', abFieldDef, 'value'), T, abValue);
        P[K] := T;
        oEntry := AB.WA.set_keyword (pcField, oEntry, P);
      }
    _skip_field:;
    }
  _skip:;
  }
  if (length (oEntry) > 2)
    return oEntry;
  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.pcPrepareOwner (
  inout data any,
  in dataValue any,
  inout meta any,
  in metaValue varchar)
{
  data := vector_concat (data, vector (dataValue));
  meta := vector_concat (meta, vector (metaValue));
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.portablecontacts () __SOAP_HTTP 'text/html'
{
  declare exit handler for sqlstate '*'
  {
    if (__SQL_STATE = '__400')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 400 Bad Request');
    }
    else if (__SQL_STATE = '__401')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 401 Unauthorized');
    }
    else if (__SQL_STATE = '__404')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 404 Not Found');
    }
    else if (__SQL_STATE = '__500')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 500 Not Found');
    }
    else if (__SQL_STATE = '__503')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 503 Service Unavailable');
    }
    else
    {
      dbg_obj_princ (__SQL_STATE, __SQL_MESSAGE);
    }
    return null;
  };
	declare uname varchar;
  declare N, M, L, domain_id, contact_id integer;
  declare filter, updatedSince, filterBy, filterOp, filterValue varchar;
  declare sort, sortBy, sortOrder varchar;
  declare startIndex, countItems varchar;
  declare fields, format varchar;
  declare tmp, pcMap, ocMap any;
  declare V, oResult, oEntries, oEntry, lines, path, params any;
  declare S, st, msg, meta, data any;

  lines := http_request_header ();
  path := http_path ();
  params := http_param ();

  domain_id := atoi (get_keyword_ucase ('inst_id', params));
	if (not ODS..ods_check_auth (uname, domain_id, 'reader'))
    signal ('__401', '');

  if (not exists (select 1 from DB.DBA.WA_INSTANCE where WAI_ID = domain_id and WAI_TYPE_NAME = 'AddressBook'))
    signal ('__401', '');

  if (path not like '/ods/portablecontacts%')
    signal ('__404', '');

    path := substring (path, length ('/ods/portablecontacts')+1, length (path));
  V := split_and_decode (trim (path, '/'), 0, '\0\0/');
  if ((length (V) < 2) or (V[0] <> '@me'))
    signal ('__404', '');

  if (V[1] not in ('@all', '@owner'))
    signal ('__404', '');

  -- output format
  format := ucase (get_keyword_ucase ('FORMAT', params, 'JSON'));
  if (format not in ('JSON', 'XML'))
    format:= 'JSON';

  -- filed mapping
  pcMap := AB.WA.pcMap ();
  -- Presentation
  fields := get_keyword_ucase ('fields', params);
  if (not isnull (fields))
  {
    fields := split_and_decode (fields, 0, '\0\0,');
  } else {
    fields := vector ();
    for (N := 0; N < length (pcMap); N := N + 2)
      fields := vector_concat (fields, vector (pcMap[N]));
  }
  set_user_id ('dba');
  oEntries := vector ();
  if (V[1] = '@owner')
  {
    st := '00000';
    S := 'select *, WA_USER_TAG_GET(U_NAME) WAUI_TAGS from DB.DBA.SYS_USERS, DB.DBA.WA_USER_INFO where WAUI_U_ID = U_ID and U_ID = ?';
    exec (S, st, msg, vector (AB.WA.domain_owner_id (domain_id)), 0, meta, data);
    if (('00000' = st) and (length (data) = 1))
    {
      data := data[0];
      meta := AB.WA.simplifyMeta (meta);
      ocMap := AB.WA.owner2contactMap();
      for (N := 0; N < length (meta); N := N + 1)
      {
        tmp := get_keyword (meta[N], ocMap);
        if (not isnull (tmp))
          meta[N] := tmp;
    }
    oEntry := AB.WA.pcContactObject (fields, data, meta, pcMap);
    if (not isnull (oEntry))
      oEntries := vector_concat (oEntries, vector (oEntry));
    }
  } else {
    contact_id := null;
    if (length (V) = 3)
    {
      contact_id := atoi (V[2]);
      if (contact_id = 0)
        signal ('__404', '');
    }

    -- filter
    filter := '';
    updatedSince := get_keyword_ucase ('UPDATEDSINCE', params);
    if (not is_empty_or_null (updatedSince))
    {
      filter := sprintf ('(cast (U_UPDATED as varchar) like ''%s%%'')', updatedSince);
    }
    filterBy := get_keyword_ucase ('FILTERBY', params);
    if (not is_empty_or_null (filterBy))
    {
      filterOp := get_keyword_ucase ('FILTEROP', params);
      if (is_empty_or_null (filterOp))
        signal ('__404', '');
      if (filterOp <> 'present')
      {
        filterValue := get_keyword_ucase ('FILTERVALUE', params);
        if (is_empty_or_null (filterValue))
          signal ('__404', '');
      }
      V := AB.WA.pcFilterField (filterBy, pcMap);
      if (not is_empty_or_null (V))
      {
        for (N := 0; N < length (V); N := N + 1)
        {
          S := '';
          if      (filterOp = 'equals')
          {
            S := sprintf ('(cast (%s as varchar) = ''%s'')', V[N], filterValue);
          }
          else if (filterOp = 'contains')
          {
            S := sprintf ('(cast (%s as varchar) like ''%%%s%%'')', V[N], filterValue);
          }
          else if (filterOp = 'startswith')
          {
            S := sprintf ('(cast (%s as varchar) like ''%s%%'')', V[N], filterValue);
          }
          else if (filterOp = 'present')
          {
            S := sprintf ('(cast (%s as varchar) <> '''')', V[N]);
          }
          if ((filter <> '') and (S <> ''))
            filter := filter || ' or ';
          filter := filter || S;
        }
      }
    }
    -- sort
    sort := '';
    sortBy := get_keyword_ucase ('SORTBY', params);
    if (not is_empty_or_null (sortBy))
    {
      sort := AB.WA.pcSortField (sortBy, pcMap);
      if (not is_empty_or_null (sort))
        sort := sort || ' ' || get_keyword_ucase ('SORTORDER', params, 'asc');
    }

    -- pagination
    startIndex := atoi (get_keyword_ucase ('STARTINDEX', params, '0'));
    countItems := atoi (get_keyword_ucase ('COUNT', params, '10'));

    st := '00000';
    S := sprintf ('select TOP %d, %d * from AB.WA.PERSONS where P_DOMAIN_ID = ?', startIndex, countItems);
    if (not is_empty_or_null (contact_id))
      S := S || ' and P_ID = ' || cast (contact_id as varchar);
    if (not is_empty_or_null (filter))
      S := S || ' and (' || filter || ')';
    if (not is_empty_or_null (sort))
      S := S || ' order by ' || sort;
    exec (S, st, msg, vector (domain_id), 0, meta, data);
    if ('00000' = st)
    {
      meta := AB.WA.simplifyMeta (meta);
      for (N := 0; N < length (data); N := N + 1)
      {
        oEntry := AB.WA.pcContactObject (fields, data[N], meta, pcMap);
        if (not isnull (oEntry))
          oEntries := vector_concat (oEntries, vector (oEntry));
      }
    }
  }
  oResult := AB.WA.apiObject ();
  oResult := vector_concat (oResult, vector ('startIndex', startIndex, 'countItems', countItems));
  if (length (oEntries))
    oResult := vector_concat (oResult, vector ('entry', oEntries));

  http_request_status ('HTTP/1.1 200 OK');
  if (format = 'JSON')
  {
    http (ODS..obj2json (oResult, 10));
  } else {
    http (AB.WA.obj2xml (oResult, 10));
  }
  return '';
}
;

grant execute on AB.WA.portablecontacts to SOAP_ADDRESSBOOK
;

-----------------------------------------------------------------------------------------
--
-- Live Contacts (Microsoft)
--
-- ---------------------------------------------------------------------------------------
create procedure AB.WA.lcPredefinedFilters (
  in lcFilter varchar)
{
  lcFilter := ucase (lcFilter);
  if (lcFilter = 'HOTMAIL')
    return 'LiveContacts(Contact(ID,CID,WindowsLiveID,AutoUpdateEnabled,AutoUpdateStatus,Profiles,Email,Phone,Location,URI),Tag)';
  if (lcFilter = 'MESSENGERSERVER')
    return 'LiveContacts(Contact(ID,CID,WindowsLiveID,AutoUpdateEnabled,AutoUpdateStatus,Profiles(Personal),Email,Phone),Tag)';
  if (lcFilter = 'MESSENGERCLIENT')
    return 'LiveContacts(Contact(ID,CID,WindowsLiveID,AutoUpdateEnabled,AutoUpdateStatus,Profiles,Email,Phone),Tag)';
  if (lcFilter = 'PHONE')
    return 'LiveContacts(Contact(ID,CID,WindowsLiveID,AutoUpdateEnabled,AutoUpdateStatus,Profiles(Personal(FirstName,LastName)),Email,Phone,Location),Tag)';
  if (lcFilter = 'MINIMALPHONE')
    return 'LiveContacts(Contact(ID,WindowsLiveID,Profiles(Personal(NameToFileAs,FirstName,LastName)),Email,Phone),Tag)';
  if (lcFilter = 'MAPPOINT')
    return 'LiveContacts(Contact(ID,WindowsLiveID,Profiles(Personal(NameToFileAs,FirstName,LastName)),Location))';
  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcFilterFields_Int (
  in lcFilter varchar,
  in lcPath varchar,
  inout lcFields any)
{
  declare L_Brace, R_Brace, L_Comma integer;

  L_Brace := strchr (lcFilter, '(');
  L_Comma := strchr (lcFilter, ',');
  while (not isnull (L_Brace) or not isnull (L_Comma))
  {
    if (not isnull (L_Comma) and (isnull (L_Brace) or (L_Comma < L_Brace)))
    {
      lcFields := vector_concat (lcFields, vector (lcPath || case when lcPath = '' then '' else '.' end || trim (subseq (lcFilter, 0, l_Comma))));
      lcFilter := trim (subseq (lcFilter, L_Comma+1, length (lcFilter)));
      goto _loop;
    }
    R_Brace := strrchr (lcFilter, ')');
    if (isnull (L_Brace) and not isnull (R_Brace))
      return;
    if (not isnull (L_Brace) and isnull (R_Brace))
      return;
    AB.WA.lcFilterFields_Int (trim (subseq (lcFilter, L_Brace+1, R_Brace)), lcPath || case when lcPath = '' then '' else '.' end || trim (subseq (lcFilter, 0, L_Brace)), lcFields);
    lcFilter := trim (subseq (lcFilter, R_Brace+1, length (lcFilter)));
  _loop:
    L_Brace := strchr (lcFilter, '(');
    L_Comma := strchr (lcFilter, ',');
  }
  if (lcFilter <> '')
    lcFields := vector_concat (lcFields, vector (lcPath || case when lcPath = '' then '' else '.' end || lcFilter));
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcFilterFields (
  in lcFilter varchar)
{
  declare lcPath, lcFields any;

  lcPath := '';
  lcFields := Vector ();
  AB.WA.lcFilterFields_Int (lcFilter, lcPath, lcFields);

  return lcFields;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcMap ()
{
  return vector ('ID',                                  vector ('field', 'P_ID'),
                 'CID',                                 vector ('field', 'P_UID'),
                 'URI',                                 vector ('field', 'P_IRI'),
                 'Profiles.Personal.NickName',          vector ('field', 'P_NAME',      'xpath', vector ('Profiles.Personal.NickName', 'Profiles.Personal.LastName', 'Profiles.Personal.FirstName')),
                 'Profiles.Personal.FirstName',         vector ('field', 'P_FIRST_NAME'),
                 'Profiles.Personal.MiddleName',        vector ('field', 'P_MIDDLE_NAME'),
                 'Profiles.Personal.LastName',          vector ('field', 'P_LAST_NAME'),
                 'Profiles.Personal.Gender',            vector ('field', 'P_GENDER'),
                 'Profiles.Personal.Birthdate',         vector ('field', 'P_BIRTHDAY',  'function', 'select subseq (AB.WA.dt_iso8601 (?), 0, 10)'),
                 'Profiles.Professional.JobTitle',      vector ('field', 'P_B_JOB'),
                 'Locations.Location.CountryRegion',    vector ('field', 'P_H_COUNTRY', 'xpath', vector ('Locations.Location[LocationType="Personal"].CountryRegion'), 'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.SubDivision',      vector ('field', 'P_H_STATE',   'xpath', vector ('Locations.Location[LocationType="Personal"].SubDivision'),   'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.PostalCode',       vector ('field', 'P_H_CODE',    'xpath', vector ('Locations.Location[LocationType="Personal"].PostalCode'),    'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.PrimaryCity',      vector ('field', 'P_H_CITY',    'xpath', vector ('Locations.Location[LocationType="Personal"].PrimaryCity'),   'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.StreetLine',       vector ('field', 'P_H_ADDRESS1','xpath', vector ('Locations.Location[LocationType="Personal"].StreetLine'),    'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.StreetLine2',      vector ('field', 'P_H_ADDRESS2','xpath', vector ('Locations.Location[LocationType="Personal"].StreetLine2'),   'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.Latitude',         vector ('field', 'P_H_LAT',     'xpath', vector ('Locations.Location[LocationType="Personal"].Latitude'),      'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.Longitude',        vector ('field', 'P_H_LNG',     'xpath', vector ('Locations.Location[LocationType="Personal"].Longitude'),     'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Personal', 'IsDefault', 'true'))),
                 'Locations.Location.CountryRegion[2]', vector ('field', 'P_B_COUNTRY', 'xpath', vector ('Locations.Location[LocationType="Business"].CountryRegion'), 'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Locations.Location.SubDivision[2]',   vector ('field', 'P_B_STATE',   'xpath', vector ('Locations.Location[LocationType="Business"].SubDivision'),   'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Locations.Location.PostalCode[2]',    vector ('field', 'P_B_CODE',    'xpath', vector ('Locations.Location[LocationType="Business"].PostalCode'),    'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Locations.Location.PrimaryCity[2]',   vector ('field', 'P_B_CITY',    'xpath', vector ('Locations.Location[LocationType="Business"].PrimaryCity'),   'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Locations.Location.StreetLine[2]',    vector ('field', 'P_B_ADDRESS1','xpath', vector ('Locations.Location[LocationType="Business"].StreetLine'),    'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Locations.Location.StreetLine2[2]',   vector ('field', 'P_B_ADDRESS2','xpath', vector ('Locations.Location[LocationType="Business"].StreetLine2'),   'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Locations.Location.Latitude[2]',      vector ('field', 'P_B_LAT',     'xpath', vector ('Locations.Location[LocationType="Business"].Latitude'),      'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Locations.Location.Longitude[2]',     vector ('field', 'P_B_LNG',     'xpath', vector ('Locations.Location[LocationType="Business"].Longitude'),     'unique', 'LocationType', 'template', vector_concat (AB.WA.apiObject (), vector ('LocationType', 'Business'))),
                 'Emails.Email.Address',                vector ('field', 'P_MAIL',      'xpath', vector ('Emails.Email[EmailType="Personal"].Address'), 'unique', 'EmailType', 'template', vector_concat (AB.WA.apiObject (), vector ('EmailType', 'Personal', 'IsDefault', 'true'))),
                 'Emails.Email.Address[2]',             vector ('field', 'P_B_MAIL',    'xpath', vector ('Emails.Email[EmailType="Business"].Address'), 'unique', 'EmailType', 'template', vector_concat (AB.WA.apiObject (), vector ('EmailType', 'Business'))),
                 'Phones.Phone.Number',                 vector ('field', 'P_PHONE',     'xpath', vector ('Phones.Phone[PhoneType="Personal"].Number'), 'unique', 'PhoneType', 'template', vector_concat (AB.WA.apiObject (), vector ('PhoneType', 'Personal', 'IsDefault', 'true'))),
                 'Phones.Phone.Number[2]',              vector ('field', 'P_B_PHONE',   'xpath', vector ('Phones.Phone[PhoneType="Business"].Number'), 'unique', 'PhoneType', 'template', vector_concat (AB.WA.apiObject (), vector ('PhoneType', 'Business'))),
                 'URIs.URI.Address',                    vector ('field', 'P_WEB',       'xpath', vector ('URIs.URI[URIType="Personal"].Address'), 'unique', 'URIType', 'template', vector_concat (AB.WA.apiObject (), vector ('URIType', 'Personal', 'IsDefault', 'true'))),
                 'URIs.URI.Address[2]',                 vector ('field', 'P_B_WEB',     'xpath', vector ('URIs.URI[URIType="Business"].Address'), 'unique', 'URIType', 'template', vector_concat (AB.WA.apiObject (), vector ('URIType', 'Business'))),
                 'Tag',                                 vector ('field', 'P_TAGS',      'type', 'tag')
                );
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcFindObject (
  in lcObject any,
  in lcProperty varchar,
  in lcUnique varchar := null,
  in lcUniqueValue varchar := null)
{
  declare N integer;

  for (N := 2; N < length (lcObject); N := N + 2)
  {
    if (lcObject[N] = lcProperty)
    {
      if (isnull (lcUnique))
        return N+1;
      if (get_keyword (lcUnique, lcObject[N+1]) = lcUniqueValue)
        return N+1;
    }
  }
  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcContactField (
  in lcField varchar,
  in lcMap varchar,
  in lcFields any := null)
{
  if (not isnull (lcFields) and not AB.WA.vector_contains (lcFields, lcField))
    return null;

  return get_keyword (lcField, lcMap);
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcContactFieldIndex (
  in abField varchar,
  in abMeta any)
{
  declare N integer;

  for (N := 0; N < length (abMeta); N := N + 1)
    if (abField = abMeta[N])
      return N;

  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcContactValue (
  in lcField varchar,
  inout data any,
  inout meta any,
  inout lcMap any)
{
  declare tmp, lcFieldDef any;
  declare abValue, abField, abFieldIndex any;

  abValue := null;
  lcFieldDef := AB.WA.lcContactField (lcField, lcMap);
  if (isnull (lcFieldDef))
    goto _skip;
  abField := get_keyword ('field', lcFieldDef);
  if (isnull (abField))
    goto _skip;
  abFieldIndex := AB.WA.lcContactFieldIndex (abField, meta);
  if (isnull (abFieldIndex))
    goto _skip;
  abValue := data[abFieldIndex];
  if (is_empty_or_null (abValue))
    goto _skip;
  if (not isnull (get_keyword ('function', lcFieldDef)))
  {
    tmp := get_keyword ('function', lcFieldDef);
    if (not isnull (tmp))
    {
      tmp := AB.WA.exec (tmp, vector (abValue));
      if (length (tmp))
        abValue := tmp[0][0];
    }
  }
_skip:;
  return abValue;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcContactObject (
  inout fields any,
  inout data any,
  inout meta any,
  inout lcMap any,
  inout lcTags any)
{
  declare L, M, N, pos integer;
  declare tmp, oEntry, oTag any;
  declare V, T, T1 any;
  declare abValue, abFieldDef, abField, abFieldType, abFieldIndex any;
  declare lcField, lcFieldDef, lcTemplate, lcUnique, lcUniqueValue any;

  oEntry := AB.WA.apiObject ();
  for (N := 0; N < length (fields); N := N + 1)
  {
    lcField := fields[N];
    abValue := AB.WA.lcContactValue (lcField, data, meta, lcMap);
    if (is_empty_or_null (abValue))
      goto _skip;
    lcFieldDef := AB.WA.lcContactField (lcField, lcMap);
    L := strchr (lcField, '[');
    if (not isnull (L))
      lcField := subseq (lcField, 0, L);
    abFieldType := get_keyword ('type', lcFieldDef);
    if (isnull (abFieldType))
    {
      if (isnull (strchr (lcField, '.')))
      {
        oEntry := vector_concat (oEntry, vector (lcField, abValue));
      }
      else
      {
        lcTemplate := get_keyword ('template', lcFieldDef, AB.WA.apiObject ());
        V := split_and_decode (lcField, 0, '\0\0.');
        if (length (V) < 2)
          goto _skip;
        if (isnull (get_keyword (V[0], oEntry)))
        {
          if (length (V) = 2)
            oEntry := vector_concat (oEntry, vector (V[0], lcTemplate));
          if (length (V) = 3)
            oEntry := vector_concat (oEntry, vector (V[0], AB.WA.apiObject ()));
        }
        T := get_keyword (V[0], oEntry);
        if (length (V) = 2)
        {
          T := AB.WA.set_keyword (V[1], T, abValue);
        }
        else if (length (V) = 3)
        {
          lcUnique := get_keyword ('unique', lcFieldDef);
          if (isnull (lcUnique))
          {
            if (isnull (get_keyword (V[1], T)))
              T := vector_concat (T, vector (V[1], lcTemplate));
            T1 := get_keyword (V[1], T);
            T1 := AB.WA.set_keyword (V[2], T1, abValue);
            T := AB.WA.set_keyword (V[1], T, T1);
          } else {
            if (not isnull (lcUnique))
              lcUniqueValue := get_keyword (lcUnique, lcTemplate);
            L := AB.WA.lcFindObject (T, V[1], lcUnique, lcUniqueValue);
            if (isnull (L))
              T := vector_concat (T, vector (V[1], lcTemplate));
            L := AB.WA.lcFindObject (T, V[1], lcUnique, lcUniqueValue);
            T[L] := AB.WA.set_keyword (V[2], T[L], abValue);
          }
        }
        oEntry := AB.WA.set_keyword (V[0], oEntry, T);
      }
    }
    else if (abFieldType = 'tag')
    {
      if (isnull (lcTags))
        lcTags := dict_new();
      tmp := AB.WA.tags2vector (abValue);
      foreach (any tag in tmp) do
      {
        tag := lcase (tag);
        oTag := dict_get(lcTags, tag, vector());
        oTag := vector_concat (oTag, vector (AB.WA.lcContactValue ('ID', data, meta, lcMap)));
        dict_put(lcTags, tag, oTag);
      }
    }
  _skip:;
  }
  if (length (oEntry) > 2)
    return oEntry;
  return null;
}
;

-----------------------------------------------------------------------------------------
--
create procedure AB.WA.lcPathAnalyze (
  in domainID integer,
  in lcPath varchar,
  in lcMethod varchar,
  inout objectType varchar,
  inout objectBodyXPath any,
  inout objectXPath varchar,
  inout objectID integer)
{
  declare I, N integer;
  declare tmp, V, A any;

  objectType := '*';
  objectXPath := '/LiveContacts';
  objectBodyXPath := vector ();
  objectID := null;

  V := split_and_decode (trim (lcPath, '/'), 0, '\0\0/');
  if ((length (V) <> 2) and (lcMethod = 'DELETE'))
    signal ('__400', '');
  if ((length (V) = 0) and (lcMethod in ('POST', 'PUT')))
    signal ('__400', '');
  if (length (V) = 0)
    return 1;
  if ((V[0] <> 'owner') and (V[0] <> 'contacts'))
    signal ('__400', '');
  if ((V[0] = 'owner') and (lcMethod = 'DELETE'))
    signal ('__400', '');
  if (V[0] = 'owner')
  {
    if (lcMethod in ('POST', 'DELETE'))
      signal ('__400', '');
    I := 1;
    objectType := 'o';
    objectXPath := objectXPath || '/Owner';
    objectBodyXPath := vector_concat (objectBodyXPath, vector ('Owner'));
  }
  else
  {
    I := 1;
    objectType := 'c';
    objectXPath := objectXPath || '/Contacts';
    objectBodyXPath := vector_concat (objectBodyXPath, vector ('Contact'));
    if ((length (V) >= 2) and (V[1] like 'contact%'))
    {
      I := 2;
      A := sprintf_inverse (V[1], 'contact(%d)', 1);
      if (length (A) <> 1)
        signal ('__404', '');
      objectID := A[0];
      if (lcMethod = 'POST')
        signal ('__400', '');
      if (not exists (select 1 from AB.WA.PERSONS where P_DOMAIN_ID = domainID and P_ID = objectID))
        signal ('__400', '');
      objectXPath := objectXPath || sprintf ('/Contact[ID=%d]', objectID);
    }
    if (isnull (ObjectID) and (lcMethod = 'PUT'))
      signal ('__400', '');
  }
  for (N := I; N < length (V); N := N + 1)
  {
    A := sprintf_inverse (V[N], '%s(%s)', 1);
    if (length (A) = 2)
    {
      tmp := get_keyword (A[0], vector ('Email', 'EmailType'));
      if (not isnull (tmp))
      {
        objectXPath := objectXPath || sprintf ('/%s[%s=\'%s\']', A[0], tmp, A[1]);
        objectBodyXPath := vector_concat (objectBodyXPath, vector (A[0]));
      goto _skip;
      }
    }
    objectXPath := objectXPath || '/' || V[N];
    objectBodyXPath := vector_concat (objectBodyXPath, vector (V[N]));
  _skip:;
  }
}
;

-- ---------------------------------------------------------------------------------------
--
create procedure AB.WA.livecontacts () __SOAP_HTTP 'text/xml'
{
  declare exit handler for sqlstate '*'
  {
    if (__SQL_STATE = '__201')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 201 Creates');
    }
    else if (__SQL_STATE = '__204')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 204 No Content');
    }
    else if (__SQL_STATE = '__400')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 400 Bad Request');
    }
    else if (__SQL_STATE = '__401')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 401 Unauthorized');
    }
    else if (__SQL_STATE = '__404')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 404 Not Found');
    }
    else if (__SQL_STATE = '__500')
    {
      AB.WA.apiHTTPError ('HTTP/1.1 500 Not Found');
    }
    else if (__SQL_STATE = '__503')
{
      AB.WA.apiHTTPError ('HTTP/1.1 503 Service Unavailable');
    }
    else
    {
      dbg_obj_princ (__SQL_STATE, __SQL_MESSAGE);
    }
    return null;
  };
  declare L, N, M, domain_id integer;
  declare xt, objectError, objectType, objectBodyXPath, objectXPath, objectID any;
  declare tmp, uname, V, A, oResult, oContacts, oContact, oTag any;
  declare lcLines, lcPath, lcParams, lcMethod, lcBody any;
  declare ocMap, filter, lcFields, lcMap, lcTags, lcField, lcFieldDef, lcMapField, lcXPath any;
  declare S, st, msg, meta, data any;

  lcLines := http_request_header ();
  lcPath := http_path ();
  lcParams := http_param ();
  lcMethod := ucase (http_request_get ('REQUEST_METHOD'));
  lcBody := string_output_string (http_body_read ());

  -- domain_id := 2;
  domain_id := atoi (get_keyword_ucase ('inst_id', lcParams));
  if (not ODS..ods_check_auth (uname, domain_id, 'reader'))
    signal ('__401', '');

  if (not exists (select 1 from DB.DBA.WA_INSTANCE where WAI_ID = domain_id and WAI_TYPE_NAME = 'AddressBook'))
    signal ('__401', '');

  if (lcPath not like '/ods/livecontacts%')
    signal ('__404', '');

  lcPath := substring (lcPath, length ('/ods/livecontacts')+1, length (lcPath));
  lcMap := AB.WA.lcMap();
  AB.WA.lcPathAnalyze (domain_id, lcPath, lcMethod, objectType, objectBodyXPath, objectXPath, objectID);

  set_user_id ('dba');
  if (lcMethod = 'GET')
  {
    filter := get_keyword ('filter', lcParams);
    tmp := AB.WA.lcPredefinedFilters (filter);
    if (not isnull (tmp))
      filter := tmp;
    lcFields := vector ();
    if (isnull (filter))
    {
      for (N := 0; N < length (lcMap); N := N + 2)
        lcFields := vector_concat (lcFields, vector (lcMap[N]));
    } else {
      tmp := AB.WA.lcFilterFields(filter);
      for (M := 0; M < length (tmp); M := M + 1)
      {
        lcField := lcase (trim (tmp[M]));
        if (lcField like 'livecontacts.%')
          lcField := subseq (lcField, length ('LiveContacts.'));
        if (lcField like 'contact.%')
          lcField := subseq (lcField, length ('Contact.'));
        if (lcField = '')
          goto _next_filterField;
        for (N := 0; N < length (lcMap); N := N + 2)
        {
          lcMapField := lcase (lcMap[N]);
          L := strchr (lcMapField, '[');
          if (not isnull (L))
            lcMapField := subseq (lcMapField, 0, L);
          if (lcMapField like lcField || '%')
            lcFields := vector_concat (lcFields, vector (lcMap[N]));
        }
      _next_filterField:;
      }
    }
    oResult := AB.WA.apiObject();
    lcTags := null;
    if ((objectType = '*') or (objectType = 'o'))
    {
      st := '00000';
      S := 'select *, WA_USER_TAG_GET(U_NAME) WAUI_TAGS from DB.DBA.SYS_USERS, DB.DBA.WA_USER_INFO where WAUI_U_ID = U_ID and U_ID = ?';
      exec (S, st, msg, vector (AB.WA.domain_owner_id (domain_id)), 0, meta, data);
      if (('00000' = st) and (length (data) = 1))
      {
        data := data[0];
        meta := AB.WA.simplifyMeta (meta);
        ocMap := AB.WA.owner2contactMap();
        for (N := 0; N < length (meta); N := N + 1)
        {
          tmp := get_keyword (meta[N], ocMap);
          if (not isnull (tmp))
            meta[N] := tmp;
        }
        oContact := AB.WA.lcContactObject (lcFields, data, meta, lcMap, lcTags);
        if (not isnull (oContact))
          oResult := vector_concat (oResult, vector ('Owner', oContact));
      }
    }
    if ((objectType = '*') or (objectType = 'c'))
    {
      st := '00000';
      S := 'select * from AB.WA.PERSONS where P_DOMAIN_ID = ?';
      if (not isnull (objectID))
        S := S || ' and P_ID = ' || cast (objectID as varchar);
      exec (S, st, msg, vector (domain_id), 0, meta, data);
      if ('00000' = st)
      {
        oContacts := AB.WA.apiObject();
        meta := AB.WA.simplifyMeta (meta);
        for (N := 0; N < length (data); N := N + 1)
        {
          oContact := AB.WA.lcContactObject (lcFields, data[N], meta, lcMap, lcTags);
          if (not isnull (oContact))
            oContacts := vector_concat (oContacts, vector ('Contact', oContact));
        }
      }
      if (length (oContacts) > 2)
        oResult := vector_concat (oResult, vector ('Contacts', oContacts));
    }
    if (not isnull (lcTags))
    {
      tmp := AB.WA.apiObject ();
      for (select p.* from AB.WA.dictionary2rs(p0)(tag varchar, IDs varchar) p where p0 = lcTags order by tag) do
      {
        A := deserialize (IDs);
        V := AB.WA.apiObject ();
        for (N := 0; N < length (A); N := N + 1)
          V := vector_concat (V, vector ('ContactID', A[N]));
        tmp := vector_concat (tmp, vector ('Tag', vector_concat (AB.WA.apiObject (), vector ('Name', tag, 'ContactIDs', V))));
      }
      if (length (tmp) > 2)
        oResult := vector_concat (oResult, vector ('Tags', tmp));
    }
    if (length (oResult) > 2)
      oResult := vector_concat (AB.WA.apiObject (), vector ('LiveContacts', oResult));
    http (AB.WA.obj2xml (oResult, 10));
  }
  else if (lcMethod = 'DELETE')
  {
    delete from AB.WA.PERSONS where P_DOMAIN_ID = domain_id and P_ID = objectID;
    signal ('__204', '');
  }
  else if ((lcMethod = 'POST') or (lcMethod = 'PUT'))
  {
    -- Insert Contact ('POST'), Update Contact/Owner ('PUT')
    declare abID, abPath, abCheckPath, abTmpPath, abNeedAdded, abField, abFields, abValue, abValues any;

    if (length (objectBodyXPath) = 1)
    {
      abNeedAdded := 1;
      abPath := '/' || objectBodyXPath[0];
    } else {
      abNeedAdded := 0;
      abPath := '';
      abCheckPath := '';
      for (N := 1; N < length (objectBodyXPath); N := N + 1)
      {
        if (N <> length (objectBodyXPath)-1)
          abPath := abPath || '/' || objectBodyXPath[N];
        abCheckPath := abCheckPath || '/' || objectBodyXPath[N];
      }
    }
    xt := xtree_doc (lcBody);
    abID := -1;
    abFields := vector ();
    abValues := vector ();
    for (N := 0; N < length (lcMap); N := N + 2)
    {
      lcField := lcMap[N];
      lcFieldDef := lcMap[N+1];
      if (isnull (lcFieldDef))
        goto _next;
      abValue := null;
      abField := get_keyword ('field', lcFieldDef);
      if (isnull (abField))
        goto _next;
      lcXPath := get_keyword ('xpath', lcFieldDef);
      if (isNull (lcXPath))
        lcXPath := vector (lcField);
      tmp := null;
      for (M := 0; M < length (lcXPath); M := M + 1)
      {
        abTmpPath := replace (lcXPath[M], '.', '/');
        if (abNeedAdded)
        {
          tmp := cast (xquery_eval (abPath || '/' || abTmpPath, xt, 1) as varchar);
        } else {
          if (abTmpPath like (trim (abCheckPath, '/') || '%'))
          {
            abTmpPath := subseq (abTmpPath, length (abPath), length (abTmpPath));
            tmp := cast (xquery_eval (abTmpPath, xt, 1) as varchar);
          }
        }
        if (not is_empty_or_null (tmp))
          goto _exit_xpath;
      }
    _exit_xpath:;
      if (is_empty_or_null (tmp))
        goto _next;
      if (abField = 'P_BIRTHDAY')
      {
        tmp := AB.WA.dt_reformat (tmp, 'YMD');
        if (is_empty_or_null (tmp))
          goto _next;
      }
      if (not AB.WA.vector_contains (abFields, abField))
      {
        abFields := vector_concat (abFields, vector (abField));
        abValues := vector_concat (abValues, vector (tmp));
      }
    _next:;
    }
    if (lcMethod = 'PUT')
    {
      if (objectType = 'c')
      {
        AB.WA.contact_update4 (objectID, domain_id, abFields, abValues, null);
      } else {
        uname := AB.WA.domain_owner_name (domain_id);
        ocMap := AB.WA.vector_reverse (AB.WA.owner2contactMap ());
        for (N := 0; N < length (abFields); N := N + 1)
        {
          tmp := get_keyword (abFields[N], ocMap);
          if (not isnull (tmp))
            WA_USER_EDIT (uname, tmp, abValues[N]);
        }
      }
      signal ('__204', '');
    } else {
      abID := AB.WA.contact_update4 (abID, domain_id, abFields, abValues, null);
      signal ('__201', '');
    }
  }
  http_request_status ('HTTP/1.1 200 OK');
  return '';

}
;

grant execute on AB.WA.livecontacts to SOAP_ADDRESSBOOK
;
