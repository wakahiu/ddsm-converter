#!/usr/bin/ruby

# This program gets a specified mammogram from the DDSM website and
# converts it to a PNG image. See the help message for full details.

require 'net/ftp'
require 'optparse'
require 'ostruct'
require 'optparse/time'
require 'pp'
require 'logger'
require 'thread'
require 'parallel'

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

class Optparse

  CODES = %w[iso-2022-jp shift_jis euc-jp utf8 binary]
  CODE_ALIASES = { "jis" => "iso-2022-jp", "sjis" => "shift_jis" }

  #
  # Return a structure describing the options.
  #
  def self.parse(args)
    # The options specified on the command line will be collected in *options*.
    # We set default values here.
    options = OpenStruct.new
	options.data = "."
	options.verbose = false
	options.all = false
	options.file = nil
	options.nthreads = 0
	
    options.inplace = false
	options.save = "."
    options.encoding = "utf8"
    options.transfer_type = :auto
	maxNthreads = 3
    

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: example.rb [options]"

      opts.separator ""
      opts.separator "Specific options:"
	  
	  # Mandatory argument.
      opts.on("-d", "--data [DATA PATH]",
              "The path containing the /DDSM directory") do |data|
        options.data = data
      end
	  
	  # Boolean switch.
      opts.on("-a", "--[no-]all", "Convert all files") do |a|
        options.all = a
      end
	  
	  
	  opts.on("-s", "--save [SAVE PATH]",
              "The path containing the results of the convertion") do |save|
        options.save = save
      end
	  
		# Boolean switch.
		opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
			options.verbose = v
		end
	  
		opts.on("-f", "--file [FIlE NAME]",
				"The filename to be converted into PNG",
				"For example: A_1141_1.LEFT_MLO",
				"Overrides --all") do |file|
			options.file = file
			if options.all or options.list != nil
				$log.error('Invalid arguments: Cannot specify a file and [--list or --all]')
			end
			options.all = false
		end
		
	  
		# List of arguments.
		opts.on("-l", "--list file_1,file_2,file_3", Array, "Example 'list' of arguments") do |list|
			options.list = list
			if options.all or options.file != nil
				$log.error('Invalid arguments: Cannot specify a list and [--file or --all]')
			end
			options.nthreads = [options.list.length, maxNthreads].min
			options.all = false
		end
		
		# Number of threads (Optional).
		opts.on("-n", "--nthreads [NUMBER OF THREADSs]",
				"The number of threads to perform conversion.",
				"0 threads denotes synchronous  code.",
				"Defaults to 0 if a single file is specified",
				"Defaults to min[8, list.length, nthreads] if list or all is specified") do |nthreads|
			if options.file != nil 
				options.nthreads = 0
			else
				if options.list == nil
					options.nthreads = [maxNthreads, nthreads.to_i].min
				else
					options.nthreads = [maxNthreads, nthreads.to_i, options.list.length].min
				end
			end
		end

		# Optional argument; multi-line description.
		opts.on("-i", "--inplace [EXTENSION]",
				"Edit ARGV files in place",
				"  (make backup if EXTENSION supplied)") do |ext|
			options.inplace = true
			options.extension = ext || ''
			options.extension.sub!(/\A\.?(?=.)/, ".")  # Ensure extension begins with dot.
		end

		# Cast 'delay' argument to a Float.
		opts.on("--delay N", Float, "Delay N seconds before executing") do |n|
		options.delay = n
		end

      # Cast 'time' argument to a Time object.
      opts.on("-t", "--time [TIME]", Time, "Begin execution at given time") do |time|
        options.time = time
      end

      # Cast to octal integer.
      opts.on("-F", "--irs [OCTAL]", OptionParser::OctalInteger,
              "Specify record separator (default \\0)") do |rs|
        options.record_separator = rs
      end

      

      # Keyword completion.  We are specifying a specific set of arguments (CODES
      # and CODE_ALIASES - notice the latter is a Hash), and the user may provide
      # the shortest unambiguous text.
      code_list = (CODE_ALIASES.keys + CODES).join(',')
      opts.on("--code CODE", CODES, CODE_ALIASES, "Select encoding",
              "  (#{code_list})") do |encoding|
        options.encoding = encoding
      end

      # Optional argument with keyword completion.
      opts.on("--type [TYPE]", [:text, :binary, :auto],
              "Select transfer type (text, binary, auto)") do |t|
        options.transfer_type = t
      end

      

      opts.separator ""
      opts.separator "Common options:"

      # No argument, shows at tail.  This will print an options summary.
      # Try it and see!
      opts.on_tail("-h", "--help", "Show this message") do
        puts opts
        exit
      end

      # Another typical switch to print the version.
      opts.on_tail("--version", "Show version") do
        puts ::Version.join('.')
        exit
      end
    end

    opt_parser.parse!(args)
    options
  end  # parse()

end  # class Optparse

# Specify the name of the info-file.
def info_file_name
  'info-file.txt'
end


# Get an FTP file as specified by a DDSM path (e.g.,
# /pub/DDSM/cases/cancers/cancer_06/case1141/A-1141-1.ics) and return the
# local path to the file, or return nil if the file could not be dowloaded.
def get_file_via_ftp(ddsm_path)
  ftp = Net::FTP.new('figment.csee.usf.edu')
  ftp.login
  ftp.passive = true
  ftp.getbinaryfile(ddsm_path)
    # Will be stored local to this program, under the same file name
    
  # Check to make sure that we managed to get the file.
  if !FileTest.exist?(File.basename(ddsm_path))
    puts "Could not get the file #{File.basename(ddsm_path)} from the DDSM FTP server; perhaps the server is busy."
    exit(-1)
  end
  
  return File.basename(ddsm_path)
end

# Check that file exists in the directory.
# e.g. /pub/DDSM/cases/cancers/cancer_06/case1141/A-1141-1.ics) and return the
# local path to the file, or return nil if the file could not be dowloaded.
def get_file_path(ddsm_path)
	
  # Check to make sure that we managed to get the file.
  if !FileTest.exist?(File.basename(ddsm_path))
    puts "Could not get the file #{File.basename(ddsm_path)} from the DDSM FTP server; perhaps the server is busy."
    exit(-1)
  end
  
  return File.basename(ddsm_path)
end


# Return the string input with the system's filesep at the end; if there
# is one there already then return input.
def ensure_filesep_terminated(input)
  if input[input.length-1].chr != File::SEPARATOR
    input += File::SEPARATOR
  end

  return input
end

# Check program input; input is the program input (i.e ARGV).
def check_inputs(input)
  if input.length != 1
    puts get_help
    exit(-1)
  end
  
  # See if the user wanted the help docs.
  if input[0] == '--help'
      puts get_help
      exit(-1)
  end
  
  # Check to make sure that the info file exists.
  if !FileTest.exist?(info_file_name)
    puts "The file #{info_file_name} does not exist; use catalogue-ddsm-ftp-server.rb"
    exit(-1)
  end

end

# Given the name of a DDSM image, return the name to the
# .ics file associated with the image name.
def get_ics_name_for_image(image_name)

  # Does image_name look right?
  if image_name[/._\d{4,4}_.\..+/].nil?
    raise 'image_name seems to be wrong. It is: ' + image_name
  end

  # Edit the image name, as .ics files have the format 'A-0384-1.ics';
  # there is no '.RIGHT_CC' (for example).
  image_name = image_name[0..(image_name.rindex('.')-1)] # Strip everything after and including the last '.'.
  image_name[1] = '-'
  image_name[6] = '-' # Change the '_'s to '-'s (better regexp-based approach?).
  image_name+='.ics' # Add the file suffix
  
  return image_name
end

# Given the name of a DDSM image, return the name to the remote
# .ics file associated with the image name. If we can't find the 
# path, then we return nil.
def get_ics_file_ftp_path(image_name)
  # Get the path to the .ics file for the specified image.
  File.open(info_file_name) do |file|
    file.each_line do |line|
      # Does this line specify the .ics file for the specified image name?
      if !line[/.+#{image_name}/].nil?
        # If so, we can stop looking
        return line
      end
    end
  end
  
  # If we get here, then we did not find a match, so we will return nil.
  return nil
end

# Given a line from a .ics file, return a string that specifies the
# number of rows and cols in the image described by the line. The
# string would be '123 456' if the image has 123 rows and 456 cols.
def get_image_dims(line)
  rows = line[/.+LINES\s\d+/][/\d+/]
  cols = line[/.+PIXELS_PER_LINE\s\d+/][/PIXELS_PER_LINE\s\d+/][/\d+/]

  return rows + ' ' + cols
end

# Given an image name and a string representing the location of a
# local .ics file, get the image dimensions and digitizer name for
# image_name. Return a hash which :image_dims maps to a string of the
# image dims (which would be '123 456' if the image has 123 rows and
# 456 cols) and :digitizer maps to the digitizer name. If we can't
# determine the dimensions and/or digitizer name, the corresponding
# entry in the hash will be nil.
def get_image_dims_and_digitizer(image_name, ics_file)
  # Get the name of the image view (e.g. 'RIGHT_CC')
  image_view = image_name[image_name.rindex('.')+1..image_name.length-1]

  image_dims = nil
  digitizer = nil

  # Read the image dimensions and digitizer name from the file.
  File.open(ics_file, 'r') do |file|
    file.each_line do |line|
      if !line[/#{image_view}.+/].nil?
        # Read the image dimensions
        image_dims = get_image_dims(line)
      end
      if !line[/DIGITIZER.+/].nil?
        # Read the digitizer type from the file.
        digitizer = line.split[1].downcase # Get the second word in the DIGITIZER line.

        # There are two types of Howtek scanner and they are
        # distinguished by the first letter in image_name.
        if digitizer == 'howtek'
          if image_name[0..0].upcase == 'A'
            digitizer += '-mgh'
          elsif image_name[0..0].upcase == 'D'
            digitizer += '-ismd'
          else
            raise 'Error trying to determine Howtek digitizer variant.'
          end
        end
      end
    end
  end

  # Return an associative array specifying the image dimensions and
  # digitizer used.
  return {:image_dims => image_dims, :digitizer =>digitizer}
end

# Given the name of a DDSM image, return a string that describes
# the image dimensions and the name of the digitizer that was used to
# capture it. If 
def do_get_image_info(base_dir, image_name)
  # Get the path to the ics file for image_name.
  ics_file_name = get_ics_name_for_image(image_name)
  ics_file_name.chomp!
  
  ics_file = File.join(base_dir, ics_file_name)
  $log.info("Reading ICS file: " + ics_file)
  # Check if the file does not exist in the base directory,
  # Fetch it via FTP
  if !FileTest.exists?(ics_file)
	$log.warn("ICS file DNE: in directory " + base_dir + ". Trying to fetch via FTP: " + ics_file_name)
	ftp_path = get_ics_file_ftp_path(ics_file_name)
	# Get the ics file; providing us with a string representing
	# the local location of the file.
	ics_file = get_file_via_ftp(ftp_path)
  end

  

  # Get the image dimensions and digitizer for image_name.
  image_dims_and_digitizer = get_image_dims_and_digitizer(image_name, ics_file)

  # Remove the .ics file as we don't need it any more.
  # File.delete(ics_file)

  return image_dims_and_digitizer
end



# Given a mammogram name and the path to the image info file, get the
# image dimensions and digitizer name string.
def get_image_info(base_dir, image_name)
	# Get the image dimensions and digitizer type for the specified
	# image as a string.
	image_info = do_get_image_info(base_dir, image_name)

	# Now output the result to standard output.
	all_ok = !image_info[:image_dims].nil? && !image_info[:digitizer].nil? # Is everything OK?
	if all_ok
		ret_val = image_info[:image_dims] + ' ' + image_info[:digitizer]
		$log.debug("Succefully read ICS file " + image_name + " : " + ret_val)
	else
		log.fatal("Could not fetch ICS for file: " + image_name)
		exit(-1)
	end

	return ret_val
end

# Return a non-existant random filename.
def get_temp_filename
  rand_name = "#{rand(10000000)}" # A longish string
  if FileTest.exist?(rand_name)
    rand_name = get_temp_filename
  end

  return rand_name
end

# Retrieve the LJPEG file for the mammogram with the specified
# image_name, given the path to the info file. Return the path to the
# local file if successful. If we can't get the file, then return nil.
def get_ljpeg(base_dir, image_name)

	file_path = File.join(base_dir, image_name + ".LJPEG")
	$log.info("Checking if LJPEG file exists: " + file_path)
	if FileTest.exist?( file_path )
		return file_path
	else
		# Get the path to the image file on the mirror of the FTP server.
		path = nil
		File.open(info_file_name) do |file|
			file.each_line do |line|
				if !line[/.+#{image_name}\.LJPEG/].nil?
					$log.warn("LJPEG file DNE: in directory " + base_dir + ". Trying to fetch via FTP: " + image_name)
					# We've found it, so get the file.
					line.chomp!
					local_path = get_file_via_ftp(line)
					return local_path
				end
			end
		end
	end
	
	# If we get here we didn't find where the file is on the server.
	$log.fatal("File does not exist: " + image_name)
	exit(-1)
end

# Given the path to the dir containing the jpeg program, the path to a
# LJPEG file, convert it to a PNM file. Return the path to the PNM
# file.
def ljpeg_to_pnm(ljpeg_file, dims_and_digitizer)
  # First convert it to raw format.
  command = "./jpeg.exe -d -s #{ljpeg_file}"
  $log.debug("Converting file to PNM: " + ljpeg_file)
  `#{command}` # Run it.
  raw_file = ljpeg_file + '.1' # The jpeg program adds a .1 suffix.
  
  # See if the .1 file was created.
  if !FileTest.exist?(raw_file)
    $log.fatal('Could not convert from LJPEG to raw.')
	exit(-1)
  end

  # Now convert the raw file to PNM and delete the raw file.
  command = "./ddsmraw2pnm.exe #{raw_file} #{dims_and_digitizer}"
  pnm_file = `#{command}`
  File.delete(raw_file)
  if $? != 0
    $log.fatal('Could not convert from raw to PNM. ' + raw_file)
	exit(-1)
  end

  # Return the path to the PNM file.
  return pnm_file.split[0]
end

# Convert a PNM file to a PNG file. pnm_file is the path to the pnm file
# and target_png_file is the name of the PNG file that we want created.
def pnm_to_png(pnm_file, target_png_file)

	$log.info("Converting file to PNG: " + pnm_file)
	command = "convert -depth 16 #{pnm_file} #{target_png_file}"
	`#{command}`

	if !FileTest.exist?(target_png_file)
		$log.error('Could not convert from PNM to PNG. ' + target_png_file)
	end

	return target_png_file
end

# Converts a single file defined in options.file to png
def convert_single_file_to_png(options)
	if options.file == nil
		$log.error("File not specified")
		return
	end
	# Get the image dimensions and digitizer name string for the
	# specified image.
	image_info = get_image_info(options.data, options.file)
	# Get the LJPEG file, returning the path to the local file.
	ljpeg_file = get_ljpeg(options.data, options.file)
	# Convert the LJPEG file to PNM
	pnm_file = ljpeg_to_pnm(ljpeg_file, image_info)
	# delete the original LJPEG.
	#File.delete(ljpeg_file)
	# Now convert the PNM file to PNG and delete the PNG file.
	target_png_file = options.file + '.png'
	png_file = pnm_to_png(pnm_file, target_png_file)
	File.delete(pnm_file)
	save_path = File.join( options.save, target_png_file)
	$log.info("Saving file to :" + save_path)
	unless options.save == "."
		command = "mv #{target_png_file} #{save_path}"
		`#{command}`
	end

	# Test to see if we got something.
	if !FileTest.exist?(png_file)
		$log.fatal( 'Could not create PNG file.' + target_png_file)
		exit(-1)
	end
	 # Display the path to the file.
	$log.info("Finished converting file: " + File.expand_path(png_file))
end

def dummy_function(options)
	$log.info(options.file)
end

def convert_list_of_files_to_png(options)
	work_q = Queue.new
	if options.list == nil
		$log.error("File list not specified")
		return
	end
	
	optArray = Array.new
	
	options.list.map! do |file_name|
		opts = OpenStruct.new(options)
		opts.file = file_name
		optArray.push(opts)
	end
	
	nproc = [options.nthreads.to_i,optArray.length].min
	$log.debug("Num Processes : #{nproc}")
	
	# N CPUs -> work in nproc processes
	results = Parallel.map(optArray, :in_processes=>nproc, :progress => "Doing stuff") do |opts|
		convert_single_file_to_png(opts)
		sleep 1
	end
	exit(0)
	
	options.list.each do |file|
		work_q.push file
	end
	
	workers = (0...options.nthreads).map do
	  Thread.new(options)  do |opts|
		begin
			while file = work_q.pop(true)
				opts.file = file
				$log.info(opts)
				convert_single_file_to_png(opts)
			end
		rescue ThreadError
		end
	  end
	end; "ok"
	workers.map(&:join); "ok"
	exit(0)

	options.list.each_with_index do |val, index|
		puts "#{index} => #{val}"
		options.file = val
		convert_single_file_to_png(options)
	end
end

def get_list_of_all_files(options)
end

# The entry point of the program.
def main

	# Parse the inputs
	options = Optparse.parse(ARGV)
	$log.info("Staring program")
	
	if options.verbose
		$log.info("OPTIONS:")
		$log.info(options)
	end
	$log.info('Looking for files from base directory: ' + options.data)
  
	if options.all == true
		image_info = get_image_info(image_name)
	elsif options.file != nil
		convert_single_file_to_png(options)
	elsif options.list != nil
		convert_list_of_files_to_png(options)
	else
		exit(-1)
	end
  

end

# The help message
def get_help
  <<END_OF_HELP

  This program gets a specified mammogram from a local mirror of the
  DDSM FTP Server, converts it to a PNG image and saves it to a target
  directory; if the target directory already contains a suitably-named
  file, the download and conversion are skipped.

Call this program using:

  ruby get-ddsm-mammo.rb <image-name>

  (Note: the '\\' simply indicates that the above command should be on
  one line.)

  where:

  * <image-name> is the name of the DDSM image you want to get and
    convert, for example: 'A_1141_1.LEFT_MLO'.

  If successful, the program will print the path to the PNG file of
  the requested mammogram to standard output and will return a status
  code of 0. If unsuccessful, the program should display a
  useful error message and return a non-zero status code.

END_OF_HELP
end

# Call the entry point.
main
