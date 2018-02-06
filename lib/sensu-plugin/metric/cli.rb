require 'sensu-plugin/cli'
require 'json'

module Sensu
  module Plugin
    class Metric
      class CLI < Sensu::Plugin::CLI
        class JSON < Sensu::Plugin::CLI
          def output(obj = nil)
            if obj.is_a?(String) || obj.is_a?(Exception)
              puts obj.to_s
            elsif obj.is_a?(Hash)
              obj['timestamp'] ||= Time.now.to_i
              puts ::JSON.generate(obj)
            end
          end
        end

        class Graphite < Sensu::Plugin::CLI
          # Outputs metrics using the Statsd datagram format
          #
          # @param args [Array<String, Int>] list of arguments
          # @note the argument order should be:
          #   `metric_path`: Mandatory, name for the metric,
          #   `value`: Mandatory, metric value
          #   `timestamp`: Optional, unix timestamp, defaults to current time
          # @return [String] formated metric data
          def output(*args)
            return if args.empty?
            if args[0].is_a?(Exception) || args[1].nil?
              puts args[0].to_s
            else
              args[2] ||= Time.now.to_i
              puts args[0..2].join("\s")
            end
          end
        end

        class Statsd < Sensu::Plugin::CLI
          # Outputs metrics using the Statsd datagram format
          #
          # @param args [Array<String, Int>] list of arguments
          # @note the argument order should be:
          #   `metric_name`: Mandatory, name for the metric,
          #   `value`: Mandatory, metric value
          #   `type`: Optional, metric type- `c` for counter, `g` for gauge, `ms` for timer, `s` for set
          # @return [String] formated metric data
          def output(*args)
            return if args.empty?
            if args[0].is_a?(Exception) || args[1].nil?
              puts args[0].to_s
            else
              type = args[2] || 'kv'
              puts [args[0..1].join(':'), type].join('|')
            end
          end
        end

        class Dogstatsd < Sensu::Plugin::CLI
          # Outputs metrics using the DogStatsd datagram format
          #
          # @param args [Array<String, Int>] list of arguments
          # @note the argument order should be:
          #   `metric_name`: Mandatory, name for the metric,
          #   `value`: Mandatory, metric value
          #   `type`: Optional, metric type- `c` for counter, `g` for gauge, `ms` for timer, `h` for histogram, `s` for set
          #   `tags`: Optional, a comma separated key:value string `tag1:value1,tag2:value2`
          # @return [String] formated metric data
          def output(*args)
            return if args.empty?
            if args[0].is_a?(Exception) || args[1].nil?
              puts args[0].to_s
            else
              type = args[2] || 'kv'
              tags = args[3] ? "##{args[3]}" : nil
              puts [args[0..1].join(':'), type, tags].compact.join('|')
            end
          end
        end

        class Influxdb < Sensu::Plugin::CLI
          # Outputs metrics using the InfluxDB line protocol format
          #
          # @param args [Array<String, Int>] list of arguments
          # @note the argument order should be:
          #   `measurement_name`: Mandatory, name for the InfluxDB measurement,
          #   `fields`: Mandatory, either an integer or a comma separated key=value string `field1=value1,field2=value2`
          #   `tags`: Optional, a comma separated key=value string `tag1=value1,tag2=value2`
          #   `timestamp`: Optional, unix timestamp, defaults to current time
          # @return [String] formated metric data
          def output(*args)
            return if args.empty?
            if args[0].is_a?(Exception) || args[1].nil?
              puts args[0].to_s
            else
              fields = if args[1].is_a?(Integer)
                         "value=#{args[1]}"
                       else
                         args[1]
                       end
              measurement = [args[0], args[2]].compact.join(',')
              ts = args[3] || Time.now.to_i
              puts [measurement, fields, ts].join(' ')
            end
          end
        end

        # hack to prevent mixlib conflict when calling JSON.output
        class J < JSON
          def parse_options(*arv)
          end
        end

        option :metric_format,
               short: '-f METRIC_FORMAT',
               long: '--metric_format METRIC_FORMAT',
               in: ['json', 'graphite', 'statsd', 'dogstatsd', 'influxdb'],
               show_options: true,
               default: 'graphite'

        def output(metric = {})
          # metric_name
          # value=nil
          # tags={}
          # timestamp=Time.now.to_i
          # graphite_metric_path="#{metric}"
          # statsd_metric_name="#{metric}"
          # statsd_type=nil
          # influxdb_measurement="#{metric}"
          # influxdb_fields="#{value}"
          # json_obj
          tags = metric[:tags] || []

          case config[:metric_format]
          when 'json'
            json_obj = metric[:json_obj] || {
              metric_name: metric[:metric_name],
              value: metric[:value],
              timestamp: metric[:timestamp],
              tags: tags
            }
            J.new.output json_obj
          when 'graphite'
            graphite_metric_path = metric[:graphite_metric_path] ||
                                   metric[:metric_name]
            Graphite.new.output graphite_metric_path, metric[:value], metric[:timestamp]
          when 'statsd'
            statsd_metric_name = metric[:statsd_metric_name] ||
                                 metric[:metric_name]
            Statsd.new.output statsd_metric_name, metric[:value], metric[:statsd_type]
          when 'dogstatsd'
            dogstatsd_metric_name = metric[:dogstatsd_metric_name] ||
                                    metric[:statsd_metric_name] ||
                                    metric[:metric_name]
            dogstatsd_type = metric[:dogstatsd_type] || metric[:statsd_type]
            dogstatsd_tags = tags.map { |k, v| "#{k}:#{v}" }.join(',')
            Dogstatsd.new.output dogstatsd_metric_name, metric[:value],
                                 dogstatsd_type, dogstatsd_tags
          when 'influxdb'
            influxdb_measurement = metric[:influxdb_measurement] ||
                                   metric[:metric_name]
            influxdb_fields = metric[:influxdb_fields] ||
                              metric[:value]
            influxdb_tags = tags.map { |k, v| "#{k}=#{v}" }.join(',')
            Influxdb.new.output influxdb_measurement, influxdb_fields,
                                influxdb_tags, metric[:timestamp]
          end
        end
      end
    end
  end
end
