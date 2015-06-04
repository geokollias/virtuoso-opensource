create function check_counts() returns varchar
{
	if ((select count(*) from post where ps_p_creatorid is null) <> 687104022)
	   return 'Wrong number of comments';
	if ((select count(*) from post_tag, post where ps_postid = pst_postid and ps_p_creatorid is null) <> 905013588)
	   return 'Wrong number of comment_tag';
	if ((select count(*) from forum) <> 10427036)
	   return 'Wrong number of forums';
	if ((select count(*) from forum_person) <> 583222245)
	   return 'Wrong number of forum_person';
	if ((select count(*) from forum_tag) <> 35869584)
	   return 'Wrong number of forum_tag';
	if ((select count(*) from organisation) <> 7996)
	   return 'Wrong number of organisations';
	if ((select count(*) from person) <> 1136127)
	   return 'Wrong number of persons';
	if ((select count(*) from person_email) <> 1940235)
	   return 'Wrong number of person_email';
	if ((select count(*) from person_tag) <> 26578675)
	   return 'Wrong number of person_tag';
	if ((select count(*)/2 from knows) <> 62624802)
	   return 'Wrong number of knows';
	if ((select count(*) from likes, post where l_postid = ps_postid and ps_p_creatorid is null) <> 816526766)
	   return 'Wrong number of person_likes_comment';
	if ((select count(*) from likes, post where l_postid = ps_postid and ps_p_creatorid is not null) <> 323699469)
	   return 'Wrong number of person_likes_post';
	if ((select count(*) from person_language) <> 2352031)
	   return 'Wrong number of person_languages';
	if ((select count(*) from person_university) <> 909401)
	   return 'Wrong number of person_university';
	if ((select count(*) from person_company) <> 2488250)
	   return 'Wrong number of person_company';
	if ((select count(*) from place) <> 1466)
	   return 'Wrong number of places';
	if ((select count(*) from post where ps_p_creatorid is not null) <> 160782622)
	   return 'Wrong number of posts';
	if ((select count(*) from post_tag, post where ps_postid = pst_postid and ps_p_creatorid is not null) <> 240880253)
	   return 'Wrong number of post_tag';
	if ((select count(*) from tag) <> 16080)
	   return 'Wrong number of tags';
	if ((select count(*) from tagclass) <> 71)
	   return 'Wrong number of tagclasses';
	if ((select count(*) from subclass) <> 70)
	   return 'Wrong number of subclasses';
	if ((select count(*) from tag_tagclass) <> 16080)
	   return 'Wrong number of tag_tagclass';

	return 'OK';
}

select check_counts();


