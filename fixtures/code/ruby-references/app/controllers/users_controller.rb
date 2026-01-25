# Controller demonstrating complex method call patterns
class UsersController < ApplicationController
  before_action :authenticate_user, except: [:create, :show]
  before_action :find_user, only: [:show, :update, :destroy]

  def index
    users = User.all
    active_users = users.select { |u| u.active? }
    
    render json: {
      users: active_users.map(&:to_h),
      total: users.count,
      active_count: active_users.count
    }
  end

  def show
    profile = @user.get_profile
    
    render json: {
      user: @user.to_h,
      profile: profile&.to_h,
      full_data: profile&.full_profile_data
    }
  end

  def create
    user = User.new(user_params)
    
    if user.save
      user.send_welcome_email
      Profile.create_default(user)
      
      render json: {
        user: user.to_h,
        message: "User created successfully"
      }, status: :created
    else
      render json: {
        errors: user.errors.full_messages
      }, status: :unprocessable_entity
    end
  end

  def update
    if @user.update(user_params)
      @user.update_profile(profile_params) if profile_params.any?
      
      render json: {
        user: @user.to_h,
        message: "User updated successfully"
      }
    else
      render json: {
        errors: @user.errors.full_messages
      }, status: :unprocessable_entity
    end
  end

  def destroy
    @user.destroy
    NotificationService.notify(@user, "Account deleted")
    
    render json: {
      message: "User deleted successfully"
    }
  end

  def activate
    user = User.find(params[:id])
    user.activate!
    
    render json: {
      user: user.to_h,
      message: "User activated"
    }
  end

  private

  def find_user
    @user = User.find(params[:id])
  rescue ActiveRecord::RecordNotFound
    render json: { error: "User not found" }, status: :not_found
  end

  def user_params
    params.require(:user).permit(:username, :email, :first_name, :last_name)
  end

  def profile_params
    params.fetch(:profile, {}).permit(:bio, :avatar_url)
  end

  def authenticate_user
    token = request.headers['Authorization']
    current_user = AuthService.authenticate_token(token)
    
    unless current_user
      render json: { error: "Unauthorized" }, status: :unauthorized
    end
  end
end
