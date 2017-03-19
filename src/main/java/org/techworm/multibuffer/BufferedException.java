/**
 * 
 */
package org.techworm.multibuffer;

/**
 * Exception that will be thrown by BufferedReadWrite in case of errors
 * @author ramkgutha
 */
public class BufferedException extends RuntimeException
{
	private static final long serialVersionUID = 1L;
	
	private boolean readError;

	/**
	 * @param message
	 * @param cause
	 */
	public BufferedException(String message, Throwable cause, boolean readError)
	{
		super(message, cause);
		this.readError = readError;
	}
	
	/**
	 * Returns if this exception is because of read error 
	 * @return the readError
	 */
	public boolean isReadError()
	{
		return readError;
	}
}
