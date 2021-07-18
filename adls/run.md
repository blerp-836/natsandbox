
##Regions load##
(python adls/main.py -j ytb_i18nRegions -a landing_load -t i18nRegions -m local&&
python adls/main.py -j ytb_i18nRegions -a staging_load_i18nRegions -t i18nRegions -m local&&
python adls/main.py -j ytb_i18nRegions -a int_load_i18nRegions -t i18nRegions -m local)

##Video Categories##
(python adls/main.py -j ytb_videoCat -a landing_load -t videoCategories -m local&&
python adls/main.py -j ytb_videoCat -a staging_load_videoCat -t videoCategories -m local&&
python adls/main.py -j ytb_videoCat -a int_load_videoCat -t videoCategories -m local)


##video##
(python adls/main.py -j ytb_video -a eventhubsend -t video -m local&&
python adls/main.py -j ytb_video -a eventhubload -t video -m local&&
python adls/main.py -j ytb_video -a int_load_video -t video -m local)


##video send streaming
python adls/main.py -j ytb_video_streaming -a eventhubstreaming -t video -m local
