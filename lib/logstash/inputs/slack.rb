# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"

class LogStash::Inputs::Slack < LogStash::Inputs::Base
  config_name "slack"
  milestone 1

  # Slack token
  config :token, :validate => :string, :required => true

  # interval token
  config :interval, :validate => :number, :default => (60 * 15)

  # get messages count (MAX 1000 message. more messages use get all messages)
  config :count, :validate => :number, :default => 1000

  # get all messages (bulk size define is get messages count parameter)
  config :getall, :validate => :boolean, :default => false

  public
  def register
    require 'rest-client'
    require 'cgi'
    require 'json'

    @content_type = "application/x-www-form-urlencoded"
  end # def register

  public
  def run(queue)
    get_channels_url = "https://slack.com/api/channels.list?token="+@token
    get_message_url = "https://slack.com/api/channels.history?count="+@count.to_s+"&token="+@token+"&channel="
    get_users_url = "https://slack.com/api/users.list?token="+@token
    p get_channels_url
    Stud.interval(@interval) do
      @logger.info('Polling Slack API')

      @users = {}
      # GET USER LIST
      begin
        # p get_users_url
        RestClient.get(
          get_users_url,
          :accept => "application/json",
          :'User-Agent' => "logstash-input-slack"
          ) { |response, request, result, &block|
            if response.code != 200
              @logger.warn("Got a #{response.code} response: #{response}")
            end
            JSON.parse(response)['members'].each do |member|
              @users[member['id']] = member['real_name']
            end
            # p @users
          }
      rescue Exception => e
        @logger.warn("Unhandled exception", :exception => e,
                    :stacktrace => e.backtrace)
      end # begin

      @channels = {}
      # GET CHANNEL LIST
      begin
        # p get_channels_url
        RestClient.get(
          get_channels_url,
          :accept => "application/json",
          :'User-Agent' => "logstash-input-slack"
          ) { |response, request, result, &block|
            if response.code != 200
              @logger.warn("Got a #{response.code} response: #{response}")
            end
            JSON.parse(response)['channels'].each do |channel|
              @channels[channel['id']] = channel['name']
            end
            # p @channels
          }
      rescue Exception => e
        @logger.warn("Unhandled exception", :exception => e,
                    :stacktrace => e.backtrace)
      end # begin

      # GET MESSAGE CHANNELS
      @channels.keys.each do |channel|
        latest_time = nil;
        begin
          loop do
            get_message_request_url = get_message_url+CGI.escape(channel)
            if (latest_time.nil?)
              latest_time = Time.now.to_f
            else
              get_message_request_url += "&latest="+latest_time
            end
            p get_message_request_url
            RestClient.get(
              get_message_request_url,
              :accept => "application/json",
              :'User-Agent' => "logstash-input-slack"
              ) { |response, request, result, &block|
                if response.code != 200
                  @logger.warn("Got a #{response.code} response: #{response}")
                end
                # p response
                if JSON.parse(response)['messages'].size <= 0
                  p "loop break"
                  break
                end
                JSON.parse(response)['messages'].each do |message|
                  # p message
                  # p LogStash::Timestamp.at(message['ts'].to_f)
                  if latest_time > message['ts'].to_f
                    latest_time = message['ts'].to_f # GET MIN TIME(for Next Request)
                  end

                  event = LogStash::Event.new(message)
                  event.set("userid", message['user'])
                  event.set("user", hash_replace(@users, message['user']))
                  event.set("host", "slack-"+@channels[channel]+"-"+event.get("user"))
                  event.set("channel", @channels[channel])
                  event.set("message", slack_replace(@users, @channels, message['text']))
                  event.set("message_raw", message['text'])
                  event.set("@timestamp", LogStash::Timestamp.at(message['ts'].to_f))
                  queue << event
                end
              }
              if !@getall
                break # ONCE EXECUTE
              end
          end
        rescue Exception => e
          @logger.warn("Unhandled exception", :exception => e,
                      :stacktrace => e.backtrace)
        end # begin
      end # for each
    end # Stud.interval
  end # def run

  private
  def slack_replace(users, channels, text) 
    return hash_replace(channels, hash_replace(users, text))
  end

  private
  def hash_replace(hash, text) 
    hash.keys.each do |key|
      # p key
      value = hash[key]
      if value == nil
        value = ""
      end
      text = text.gsub(key, value)
    end
    return text
  end
end # class LogStash::Inputs::Slack
