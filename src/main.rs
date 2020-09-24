use std::fs::{self};
use std::path::{PathBuf, Path};
use std::process::Command;
use std::sync::atomic::{Ordering,AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::cell::UnsafeCell;

#[derive(Clone)]
pub struct MultiSliceReadWriteLock<T> {

    data: Arc<UnsafeCell<T>>
}

unsafe impl<T> Send for MultiSliceReadWriteLock<T> {}
unsafe impl<T> Sync for MultiSliceReadWriteLock<T> {}

impl<T> MultiSliceReadWriteLock<T> {
    
    pub fn new(data: T) -> MultiSliceReadWriteLock<T> {
        MultiSliceReadWriteLock {
            data: Arc::new(UnsafeCell::new(data))
        }    
    }
    
    pub fn write(&self) -> &mut T {
        // TODO(SS): Ensure no one else can grab reference to same slice twice
        unsafe {  &mut *self.data.get() }
    }
    
    pub fn read(&self) -> &T {
        // TODO(SS): Ensure no one can read when write is checked out?
        unsafe {  & *self.data.get() }
    }
}

fn main() {
    
    let matches = clap::App::new("Flatten")
        .about("Flattens symlinks into files")
        .arg(clap::Arg::new("DIRECTORY")
            .about("Directory to flatten recursively")
            .index(1)
            .takes_value(true)
            .required(true))
        .arg(clap::Arg::new("SKIP_DIRS")
            .about("Directories to skip, matches partial name")
            .long("skip_dir")
            .short('s')
            .takes_value(true)
            .multiple(true))
        .get_matches();
    
    let folder = Path::new(matches.value_of("DIRECTORY").unwrap());
    if !folder.is_dir() {
        println!("Directory does not exist");
        return;
    }

    println!("Gathering symlinks recursively for {}", folder.display());

    let num_cpus = 1;//num_cpus::get() * 2;
    let should_exit = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicUsize::new(0));
    let bytes_copied = Arc::new(AtomicUsize::new(0));

    // Collect directories to skip
    let skip_dirs = if let Some(skip_dirs) = matches.values_of("SKIP_DIRS") {
        println!("Skipping directories containing:");
        let mut skip_dirs_vec = vec![];
        skip_dirs.clone().for_each(|d| {
            println!("\t{}", d);
            skip_dirs_vec.push(d);
        });
        skip_dirs_vec
    } else {
        vec![]
    };

    

    // Rather that doing read_dir which evaluates all symlinks and takes ages, run the dir command which is super fast
    // and parse the output to build up all symlinks.
    let output = Command::new("cmd").current_dir(folder).args(&["/C","dir /s"]).output().expect("failed to execute");
    let output_str = String::from_utf8_lossy(&output.stdout);
    let mut current_dir: String = String::from("");
    let mut symlinks: Vec<PathBuf> = Vec::new();
    let mut num_dirs = 0;
    let mut skip_directory = false;
    for line in output_str.lines() {
        if let Some(index) = line.find("Directory of ") {
            let dir = String::from(line);
            current_dir = String::from(&dir[index as usize + "Directory of ".len() .. dir.len()]) + "\\";
            num_dirs += 1;
            skip_directory = false;
            for s in &skip_dirs {
                if current_dir.contains(s) {
                    skip_directory = true;
                }
            }
        } else if let Some(index) = line.find("<SYMLINK>") {
            if !skip_directory {
                let file = String::from(line);
                let mut filepath = current_dir.clone() + String::from(&file[index as usize + "<SYMLINK>".len() .. file.len()]).trim_start();
                if let Some(index) = filepath.find(" [\\\\") {
                    filepath = String::from(&filepath[0 .. index as usize]);
                }
                let filepath = PathBuf::from(&filepath);
                symlinks.push(filepath);
            }
        }
    }
    
    // listen for ctrl+c to shutdown cleanly
    {
        let should_exit = should_exit.clone();
        ctrlc::set_handler(move || {
            println!("Exiting..");
            should_exit.store(true, Ordering::SeqCst)
        })
        .expect("Error setting Ctrl-C handler")
    }

    let num_symlinks = symlinks.len();
    let symlinks = Arc::new(MultiSliceReadWriteLock::new(symlinks));
    let mut threads = vec![];
    
    if num_symlinks == 0 {
        println!("No symlinks found, exiting");
        return;
    }

    println!("Found {} symlinks in {} directories", num_symlinks, num_dirs);
    println!("Do you wish to continue? (y/n)");
    let mut input = String::new();
    if std::io::stdin().read_line(&mut input).is_ok() {
        if input.to_lowercase().starts_with("y") {
            println!("Processing on {} threads", num_cpus);
            for _ in 1..num_cpus {
                let symlinks = symlinks.clone();
                let should_exit = should_exit.clone();
                let counter = counter.clone();
                let bytes_copied = bytes_copied.clone();
                let thread_handle = std::thread::spawn(move || {
                    process_symlinks(symlinks, should_exit, counter, bytes_copied);
                });
                threads.push(thread_handle);
            }
        
            process_symlinks(symlinks, should_exit, counter, bytes_copied);
            
            for thread in threads {
                let _ = thread.join();
            }
        }
    }

    println!("Done");
}

fn process_symlinks(
    symlinks: Arc<MultiSliceReadWriteLock<Vec<PathBuf>>>, 
    should_exit: Arc<AtomicBool>, 
    counter: Arc<AtomicUsize>,
    bytes_copied: Arc<AtomicUsize>) {

    let mut index;
    let num_symlinks = symlinks.read().len();
    while {index = counter.fetch_add(1, Ordering::SeqCst); index } < num_symlinks &&  !should_exit.load(Ordering::SeqCst) {
        process_symlink(&symlinks.write()[index], index, num_symlinks, &should_exit, 0, &bytes_copied);
    }
}

fn process_symlink(
    symlink: &PathBuf, 
    index: usize, 
    total: usize, 
    should_exit: &AtomicBool, 
    depth: u32, 
    bytes_copied: &AtomicUsize) {

    if should_exit.load(Ordering::SeqCst) {
        return;
    }

    // Process
    // 1. Copy file into symlink.ext.temp
    //   a. Copy fails delete symlink.ext.temp if exists
    //   b. If network error retry in 10 seconds after vpn has reconnected
    // 2. Rename symlink.ext.temp to symlink.ext

    println!("Resolving symlink ({}/{}) {}", index+1, total, symlink.display());
    // get current extension
    let extension = osstr_to_string_safe(symlink.extension());
    // create new extension.temp extension so we know what type it was originally
    let temp_extension = extension + ".temp";
    // create new symlink temp path with temp extension so we can rename it
    let symlink_temp = symlink.as_path().with_extension(&temp_extension);
    // copy file from server into temp
    let copy_result = fs::copy(&symlink, &symlink_temp);
    if let Ok(file_size_bytes) = copy_result {
        
        // clear read-only file otherwise we can't rename it
        let metadata = fs::metadata(&symlink_temp).unwrap();
        let mut permissions = metadata.permissions();
        let is_read_only = permissions.readonly();
        permissions.set_readonly(false);
        let _ = fs::set_permissions(&symlink, permissions.clone());

        let mut flatten_success = true;

        // Rename downloaded temp file to correct file
        if let Err(e) = fs::rename(&symlink_temp, &symlink) {
            println!("Failed to copy {}. Error: {}", symlink_temp.display(), e);
            flatten_success = false;
        }

        // Restore read only flag
        permissions.set_readonly(is_read_only);
        let _ = fs::set_permissions(&symlink, permissions);

        if flatten_success {
            let total_bytes_copied = bytes_copied.fetch_add(file_size_bytes as usize, Ordering::SeqCst) + file_size_bytes as usize;
            const ONE_MB: f64 = 1024.0*1024.0;
            println!("Flattened symlink ({}/{}) {}", index+1, total, symlink.display());
            println!("Total Bytes Copied {:.4} MB", total_bytes_copied as f64 / ONE_MB);
        }

    } else {

        let error = copy_result.err().unwrap();
        println!("Failed to copy {}: {}", symlink.display(), error);
        // copy failed, delete temp file 
        let _ = fs::remove_file(symlink_temp);

        // If it was a network error lets try again
        if let Some(error_code) = error.raw_os_error() {
            const NETWORK_PATH_NOT_FOUND: i32 = 53;
            const NETWORK_NAME_NOT_FOUND: i32 = 67;
            if error_code == NETWORK_PATH_NOT_FOUND || error_code == NETWORK_NAME_NOT_FOUND {
                if depth < 3 {
                    println!("Network Error for {}: Waiting 10 seconds and trying again", symlink.display());
                    std::thread::sleep(std::time::Duration::from_secs(10));
                    process_symlink(symlink, index, total, should_exit, depth+1, bytes_copied);
                } else {
                    println!("Retries exceed, exiting..");
                    //should_exit.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
    }

    return;
}

fn osstr_to_string_safe(os_str_wrapped: Option<&std::ffi::OsStr>) -> String {
    let mut string = String::from("");
    if let Some(os_str) = os_str_wrapped {
        string = os_str.to_string_lossy().into_owned();
    }
    string
}