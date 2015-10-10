require 'twitter'
require 'pp'
require 'sqlite3'
require 'amazon/ecs'
require 'pg'



#キー等の登録
client_rest = Twitter::REST::Client.new do |config|
  config.consumer_key        = "dsFcWBVjn8BS6gyKv2q6X0QSw"
  config.consumer_secret     = "oTRyRiDash7XUfmwmJGrg1LXbeOr1jjtzZDDECt0MSuLAzDx67"
  config.access_token        = "3758421078-JoPRu04i3hawTTwmw9ThjQ7O8ZGLy5HzBYgzmME"
  config.access_token_secret = "y8EAWO1r4pBKTSvzfKh9itYfnsmaRVNAlSi5r8SfwyEQk"
end

client_stream = Twitter::Streaming::Client.new do |config|
  config.consumer_key        = "dsFcWBVjn8BS6gyKv2q6X0QSw"
  config.consumer_secret     = "oTRyRiDash7XUfmwmJGrg1LXbeOr1jjtzZDDECt0MSuLAzDx67"
  config.access_token        = "3758421078-JoPRu04i3hawTTwmw9ThjQ7O8ZGLy5HzBYgzmME"
  config.access_token_secret = "y8EAWO1r4pBKTSvzfKh9itYfnsmaRVNAlSi5r8SfwyEQk"
end

Amazon::Ecs.configure do |options|
  options[:AWS_access_key_id] = "AKIAJWQHJ6XGHKTZCMHA"
  options[:AWS_secret_key]    = "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"
  options[:associate_tag]     = "m035f-22"
end


#ここからメソッド定義
def user_exist?(object, db)
  screen_name = object.user.screen_name
  if db.exec("select id from users where screen_name='#{screen_name}';").to_a.empty?
    return false
  else 
    return true
  end
end

def regist_sql(user_name, item_name, db)
  user_id = db.exec("select id from users where screen_name = '#{user_name}';").to_a[0].values.shift.to_i
  p user_id
  Amazon::Ecs.configure do |options|
    options[:AWS_access_key_id] = "AKIAJWQHJ6XGHKTZCMHA"
    options[:AWS_secret_key]    = "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"
    options[:associate_tag]     = "m035f-22"
  end


  response1 = Amazon::Ecs.item_search(item_name , 
                              :search_index => 'All' , 
                              :response_group => 'Medium' , 
                              :country => 'jp',
                              :ItemPage => '1')

  amazon_item       = response1.items.first
  title        = amazon_item.get('ItemAttributes/Title')
  asin         = amazon_item.get('ASIN')
  puts "OK"
  p db.exec("select id from items where asin = '#{asin}';").to_a
  if db.exec("select id from items where asin = '#{asin}';").to_a.empty?
  #もし、データベースに存在していなかったらitemsテーブルに情報を保存
    small_image  = amazon_item.get("SmallImage/URL")
    medium_image = amazon_item.get("MediumImage/URL")
    large_image  = amazon_item.get("LargeImage/URL")
    detail_page_url = amazon_item.get("DetailPageURL")
    raw_info        = amazon_item.get_hash
    description = nil
    created_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")
    updated_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")
    id = db.exec("select id from items order by updated_at desc;").to_a[0].values.shift.to_i
    id += 1;
    db.exec("insert into items (asin,title,description,detail_page_url,small_image,medium_image,large_image,raw_info,created_at,updated_at) values ('#{asin}', '#{title}' ,'#{description}' , '#{detail_page_url}', '#{small_image}','#{medium_image}','#{large_image}', '#{raw_info}', '#{created_at}', '#{updated_at}');")
    puts "登録完了"
  end
  #ここから、欲しいものリストに追加処理
  #もしownershipsテーブルに存在していればなにもしない
  item_id = db.exec("select id from items where title ='#{title}'").to_a[0].values.shift.to_i
  puts "item_id=#{item_id}"
  puts "user_id=#{user_id}"
  p db.exec("select id from ownerships where user_id='#{user_id}' and item_id='#{item_id}' and type='Want'").to_a
  if db.exec("select id from ownerships where user_id='#{user_id}' and item_id='#{item_id}' and type='Want'").to_a.empty?
    #id|user_id|item_id|type|created_at|updated_at
    puts "regist"
    created_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")
    updated_at = Time.now.strftime("%Y-%m-%d %H:%M:%S")
#    id = db.exec("select id  from ownerships order by updated_at desc;").to_a[0].values.shift.to_i + 1
    db.exec("insert into ownerships (user_id ,item_id, type, created_at,updated_at) values ('#{user_id}', '#{item_id}' ,'Want' ,'#{created_at}', '#{updated_at}');")
  else
    puts "exist"
  end
end


def user_exist?(object, db)
  screen_name = object.user.screen_name
  if db.exec("select id from users where screen_name='#{screen_name}';").to_a.empty?
    return false
  else 
    return true
  end
end

def parse_tweet(tweet)
  text = tweet.text
  words = text.split(/\s/)
  return words
end


#ここからプログラム
db =  PG::connect(:host => 'ec2-54-197-241-239.compute-1.amazonaws.com', :user => 'styrdvehetgpob', :password => '0leNDKSqENynNM_tmvVPjZrMdJ', :dbname => 'd88rkfagj4j3s2', :port => "5432")
bot_id = "omiyage_list"

client_stream.user do |object|
  case object
  when Twitter::Tweet
    if(/@omiyage_list/ =~ object.text)
      #botにリプライきてから処理開始 
      option = { 'in_reply_to_status_id' => object.id }
      if user_exist?(object,db)
        #ユーザが登録されていたら処理開始
        word_array = parse_tweet(object)
        word_array.reject!{|name| name =~ /@#{bot_id}/}
        if word_array.select{|name| name =~ /^@/}.size == 0 
          puts word_array
          want_list = word_array.join("と")
          msg = "@#{object.user.screen_name} " + want_list + "を欲しいものリストに追加しました。 http://tanomiyage.herokuapp.com/"
        elsif word_array.select{|name| name =~ /^@/}.size == 1
          request_id = word_array.select{|name| name =~ /^@/}.join
          word_array.reject!{|name| name =~ /^@/}
          want_list = word_array.join("と")
          msg = "@#{object.user.screen_name}さんが " + want_list + " を"+"#{request_id}さんにリクエストしました。 http://tanomiyage.herokuapp.com/"
        else
          msg = "@#{object.user.screen_name} 二人以上の人に同時にお土産リクエストはできません。" 
        end
        puts "#{word_array.size}この処理の登録処理を行います"
        word_array.size.times do |i|
          regist_sql(object.user.screen_name,word_array[i],db)
        end
        
      else
        #登録されてなかったら登録するようにherokuのアドレスを送る
        msg = "@#{object.user.screen_name} お土産リクエストがきましたが、まだあなたはたのみやげに登録されていないようです。 http://tanomiyage.herokuapp.com/"
      end

      client_rest.update msg,option
    end
  end
end


db.finish