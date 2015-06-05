echo "comment"
zcat outputDir/comment_?_0.csv.gz outputDir/comment_??_0.csv.gz | wc -l
echo "comment_hasTag_tag"
zcat outputDir/comment_hasTag_tag_?_0.csv.gz outputDir/comment_hasTag_tag_??_0.csv.gz | wc -l
echo "forum"
zcat outputDir/forum_?_0.csv.gz outputDir/forum_??_0.csv.gz | wc -l
echo "forum_hasMember_person"
zcat outputDir/forum_hasMember_person_?_0.csv.gz outputDir/forum_hasMember_person_??_0.csv.gz | wc -l
echo "forum_hasTag_tag"
zcat outputDir/forum_hasTag_tag_?_0.csv.gz outputDir/forum_hasTag_tag_??_0.csv.gz | wc -l
echo "organisation"
zcat outputDir/organisation_?_0.csv.gz outputDir/organisation_??_0.csv.gz | wc -l
echo "person"
zcat outputDir/person_?_0.csv.gz outputDir/person_??_0.csv.gz | wc -l
echo "person_email_emailaddress"
zcat outputDir/person_email_emailaddress_?_0.csv.gz outputDir/person_email_emailaddress_??_0.csv.gz | wc -l
echo "person_hasInterest_tag"
zcat outputDir/person_hasInterest_tag_?_0.csv.gz outputDir/person_hasInterest_tag_??_0.csv.gz | wc -l
echo "person_knows_person"
zcat outputDir/person_knows_person_?_0.csv.gz outputDir/person_knows_person_??_0.csv.gz | wc -l
echo "person_likes_comment"
zcat outputDir/person_likes_comment_?_0.csv.gz outputDir/person_likes_comment_??_0.csv.gz | wc -l
echo "person_likes_post"
zcat outputDir/person_likes_post_?_0.csv.gz outputDir/person_likes_post_??_0.csv.gz | wc -l
echo "person_speaks_language"
zcat outputDir/person_speaks_language_?_0.csv.gz outputDir/person_speaks_language_??_0.csv.gz | wc -l
echo "person_studyAt_organisation"
zcat outputDir/person_studyAt_organisation_?_0.csv.gz outputDir/person_studyAt_organisation_??_0.csv.gz | wc -l
echo "person_workAt_organisation"
zcat outputDir/person_workAt_organisation_?_0.csv.gz outputDir/person_workAt_organisation_??_0.csv.gz | wc -l
echo "place"
zcat outputDir/place_?_0.csv.gz outputDir/place_??_0.csv.gz | wc -l
echo "post"
zcat outputDir/post_?_0.csv.gz outputDir/post_??_0.csv.gz | wc -l
echo "post_hasTag_tag"
zcat outputDir/post_hasTag_tag_?_0.csv.gz outputDir/post_hasTag_tag_??_0.csv.gz | wc -l
echo "tag"
zcat outputDir/tag_?_0.csv.gz outputDir/tag_??_0.csv.gz | wc -l
echo "tagclass"
zcat outputDir/tagclass_?_0.csv.gz outputDir/tagclass_??_0.csv.gz | wc -l
echo "tagclass_isSubclassOf_tagclass"
zcat outputDir/tagclass_isSubclassOf_tagclass_?_0.csv.gz outputDir/tagclass_isSubclassOf_tagclass_??_0.csv.gz | wc -l
echo "tag_hasType_tagclass"
zcat outputDir/tag_hasType_tagclass_?_0.csv.gz outputDir/tag_hasType_tagclass_??_0.csv.gz | wc -l
